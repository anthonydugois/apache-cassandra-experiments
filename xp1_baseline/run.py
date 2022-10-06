import logging
import pathlib
import shutil
import time
from datetime import datetime

import enoslib as en
import pandas as pd

from drivers.Cassandra import Cassandra
from drivers.NoSQLBench import NoSQLBench, RunCommand
from drivers.Resources import Resources

ROOT = pathlib.Path(__file__).parent

DEFAULT_JOB_NAME = "cassandra"
DEFAULT_SITE = "nancy"
DEFAULT_CLUSTER = "gros"
DEFAULT_ENV_NAME = "debian11-x64-min"
DEFAULT_WALLTIME = "00:30:00"
DEFAULT_OUTPUT = str(ROOT / "output" / f"{ROOT.name}.{datetime.now().isoformat(timespec='seconds')}")
DEFAULT_RAMPUP_RATE = 50_000
DEFAULT_REPORT_INTERVAL = 30
DEFAULT_HISTOGRAM_FILTER = ".*result:30s"

DSTAT_SLEEP_IN_SEC = 5


def run(site: str,
        cluster: str,
        settings: dict,
        parameters: pd.DataFrame,
        filtered_parameters: pd.DataFrame,
        output_path: str,
        rampup_rate=DEFAULT_RAMPUP_RATE,
        report_interval=DEFAULT_REPORT_INTERVAL,
        histogram_filter=DEFAULT_HISTOGRAM_FILTER):
    _output_path = pathlib.Path(output_path)

    parameters.to_csv(_output_path / "input.all.csv")
    filtered_parameters.to_csv(_output_path / "input.csv")

    _raw_path = _output_path / "raw"
    _raw_path.mkdir(parents=True, exist_ok=True)

    # Warning: the two following values must be wrapped in an int, as pandas returns an np.int64, which is not usable in
    # the resource driver.
    max_hosts = int(filtered_parameters["hosts"].max())
    max_clients = int(filtered_parameters["clients"].max())

    # Acquire G5k resources
    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["nodes", "cassandra"], max_hosts)
    resources.add_machines(["nodes", "clients"], max_clients)

    resources.acquire(with_docker="nodes")

    # Run experiments
    for _id, params in filtered_parameters.iterrows():
        _name = params["name"]
        _repeat = params["repeat"]
        _version = params["version"]
        _hosts = params["hosts"]
        _clients = params["clients"]
        _ops = params["ops"]
        _throughput = params["throughput"]
        _throughput_ref = params["throughput_ref"]
        _rf = params["rf"]
        _value_size_in_bytes = params["value_size_in_bytes"]
        _bytes_per_host = params["bytes_per_host"]
        _docker_image = params["docker_image"]
        _config_file = params["config_file"]
        _driver_config_file = params["driver_config_file"]
        _workload_file = params["workload_file"]

        logging.info(f"Preparing {_name}#{_id}...")

        _current_path = _raw_path / _name
        _config_path = _current_path / "config"
        _config_path.mkdir(parents=True, exist_ok=True)

        rf = min(_rf, _hosts)
        bytes_total = _hosts * _bytes_per_host / rf
        key_count = round(bytes_total / _value_size_in_bytes)

        # Infer saturating throughput
        sat_throughput = 0
        if not pd.isna(_throughput_ref):
            ref_match = parameters[parameters.index == _throughput_ref]

            if ref_match.empty:
                logging.error(f"Reference #{_throughput_ref} does not exist.")
            else:
                ref_row = ref_match.iloc[0]
                ref_path = _raw_path / ref_row["name"]

                if ref_path.exists():
                    # Retrieve saturating throughput
                    ref_df = pd.read_csv(ref_path / "data" / "csv" / f"{ROOT.name}.result.csv", index_col=False)
                    sat_throughput = ref_df[ref_df["count"] >= ref_row["ops"]].iloc[0]["mean_rate"]

                    logging.info(f"Saturating throughput currently set to {sat_throughput}.")
                else:
                    logging.warning(f"Reference #{_throughput_ref} has not been executed yet."
                                    "Could not infer saturating throughput.")

        cassandra_hosts = resources.roles["cassandra"][:_hosts]
        nb_hosts = resources.roles["clients"][:_clients]

        cassandra_config_path = ROOT / "config" / "cassandra"
        nb_driver_path = ROOT / "config" / "driver"
        nb_workload_path = ROOT / "config" / "workloads"

        cassandra_config_file = cassandra_config_path / _config_file
        nb_driver_config_file = nb_driver_path / _driver_config_file
        nb_workload_file = nb_workload_path / _workload_file

        # Save config
        shutil.copy(cassandra_config_file, _config_path / "cassandra.yaml")
        shutil.copy(nb_driver_config_file, _config_path / "nb-driver.json")
        shutil.copy(nb_workload_file, _config_path / "nb-workload.yaml")

        # Save input parameters
        input_row = parameters[parameters.index == _id]
        input_row.to_csv(_current_path / "input.csv")

        logging.info(f"Running {_name}#{_id}.")

        for run_index in range(_repeat):
            _run_path = _current_path / f"run-{run_index}"
            _run_path.mkdir(parents=True, exist_ok=True)

            # Deploy and start Cassandra
            cassandra = Cassandra(name="cassandra", docker_image=_docker_image)

            cassandra.init(cassandra_hosts)
            cassandra.create_config(cassandra_config_file)
            cassandra.deploy_and_start()

            logging.info(cassandra.nodetool("status"))

            # Deploy NoSQLBench
            nb = NoSQLBench(name="nb",
                            docker_image="nosqlbench/nosqlbench:nb5preview",
                            driver_path=nb_driver_path,
                            workload_path=nb_workload_path)

            nb.init(nb_hosts)
            nb.deploy()

            # Create schema
            schema_options = dict(driver="cqld4",
                                  workload=nb.workload(nb_workload_file.name),
                                  alias=ROOT.name,
                                  tags="block:schema",
                                  driverconfig=nb.driver(nb_driver_config_file.name),
                                  threads=1,
                                  rf=rf,
                                  host=cassandra.get_host_address(0),
                                  localdc="datacenter1")

            schema_cmd = RunCommand.from_options(**schema_options)

            nb.command(schema_cmd, name="nb-schema")

            # Insert data
            rampup_options = dict(driver="cqld4",
                                  workload=nb.workload(nb_workload_file.name),
                                  alias=ROOT.name,
                                  tags="block:rampup",
                                  driverconfig=nb.driver(nb_driver_config_file.name),
                                  threads="auto",
                                  cycles=key_count,
                                  cyclerate=rampup_rate,
                                  keycount=key_count,
                                  valuesize=_value_size_in_bytes,
                                  host=cassandra.get_host_address(0),
                                  localdc="datacenter1")

            rampup_cmd = RunCommand.from_options(**rampup_options)

            nb.command(rampup_cmd, name="nb-rampup")

            logging.info(cassandra.nodetool("tablestats baselines.keyvalue"))
            logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

            # Main experiment
            main_options = dict(driver="cqld4",
                                workload=nb.workload(nb_workload_file.name),
                                alias=ROOT.name,
                                tags="block:main-read",
                                driverconfig=nb.driver(nb_driver_config_file.name),
                                threads="auto",
                                cycles=_ops,
                                keycount=key_count,
                                host=cassandra.get_host_address(0),
                                localdc="datacenter1")

            if sat_throughput > 0:
                main_options["cyclerate"] = _throughput * sat_throughput

            main_cmd = RunCommand \
                .from_options(**main_options) \
                .logs_dir(nb.data()) \
                .log_histograms(nb.data(f"/histograms.csv:{histogram_filter}")) \
                .log_histostats(nb.data(f"/histostats.csv:{histogram_filter}")) \
                .report_summary_to(nb.data("/summary.txt")) \
                .report_csv_to(nb.data("/csv")) \
                .report_interval(report_interval)

            _tmp_dstat_path = _run_path / "dstat"
            with en.Dstat(nodes=[*cassandra.hosts, *nb.hosts], options="-aT", backup_dir=_tmp_dstat_path):
                time.sleep(DSTAT_SLEEP_IN_SEC)  # Make sure Dstat is running when we start main experiment
                nb.command(main_cmd, name="nb-main")
                time.sleep(DSTAT_SLEEP_IN_SEC)  # Let the system recover before killing Dstat

            # Save results
            _client_path = _run_path / "clients"
            _client_dstat_path = _client_path / "dstat"
            _client_dstat_path.mkdir(parents=True, exist_ok=True)

            _host_path = _run_path / "hosts"
            _host_dstat_path = _host_path / "dstat"
            _host_dstat_path.mkdir(parents=True, exist_ok=True)

            for client in nb.hosts:
                _dstat_dir = _tmp_dstat_path / client.address
                if _dstat_dir.exists():
                    for _dstat_file in _dstat_dir.rglob("*-dstat.csv"):
                        shutil.copy(_dstat_file, _client_dstat_path / f"{client.address}-{_dstat_file.name}")
                else:
                    logging.warning(f"{_dstat_dir} does not exist.")

            for host in cassandra.hosts:
                _dstat_dir = _tmp_dstat_path / host.address
                if _dstat_dir.exists():
                    for _dstat_file in _dstat_dir.rglob("*-dstat.csv"):
                        shutil.copy(_dstat_file, _host_dstat_path / f"{host.address}-{_dstat_file.name}")
                else:
                    logging.warning(f"{_dstat_dir} does not exist.")

            shutil.rmtree(_tmp_dstat_path)

            with (_host_path / "cassandra.log").open("w") as file:
                file.write(cassandra.logs())

            nb.sync_results(_run_path)

            # Destroy instances
            logging.info("Destroying instances...")

            nb.destroy()
            cassandra.destroy()

    # Release resources
    resources.release()


if __name__ == "__main__":
    import argparse

    from sys import stdout
    from enoslib.config import set_config

    set_config(ansible_stdout="noop")
    logging.basicConfig(stream=stdout, level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")

    parser = argparse.ArgumentParser()

    parser.add_argument("input", type=str)
    parser.add_argument("--job-name", type=str, default=DEFAULT_JOB_NAME)
    parser.add_argument("--site", type=str, default=DEFAULT_SITE)
    parser.add_argument("--cluster", type=str, default=DEFAULT_CLUSTER)
    parser.add_argument("--env-name", type=str, default=DEFAULT_ENV_NAME)
    parser.add_argument("--reservation", type=str, default=None)
    parser.add_argument("--walltime", type=str, default=DEFAULT_WALLTIME)
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT)
    parser.add_argument("--rampup-rate", type=int, default=DEFAULT_RAMPUP_RATE)
    parser.add_argument("--report-interval", type=int, default=DEFAULT_REPORT_INTERVAL)
    parser.add_argument("--histogram-filter", type=str, default=DEFAULT_HISTOGRAM_FILTER)
    parser.add_argument("--id", type=str, action="append", default=None)
    parser.add_argument("--from-id", type=str, default=None)
    parser.add_argument("--to-id", type=str, default=None)

    args = parser.parse_args()

    parameters = pd.read_csv(args.input, index_col="id")

    if args.from_id is None and args.to_id is None:
        if args.id is None:
            rows = parameters
        else:
            rows = parameters[parameters.index.isin(args.id)]
    else:
        from_index = 0
        if args.from_id is not None:
            from_index = parameters.index.get_loc(args.from_id)

        to_index = len(parameters.index)
        if args.to_id is not None:
            to_index = parameters.index.get_loc(args.to_id)

        ids = list(parameters.index[from_index:to_index])
        if args.id is not None:
            ids.extend(args.id)

        rows = parameters[parameters.index.isin(ids)]

    settings = dict(job_name=args.job_name, env_name=args.env_name, walltime=args.walltime)

    if args.reservation is not None:
        settings["reservation"] = args.reservation

    run(site=args.site,
        cluster=args.cluster,
        settings=settings,
        parameters=parameters,
        filtered_parameters=rows,
        output_path=args.output,
        rampup_rate=args.rampup_rate,
        report_interval=args.report_interval,
        histogram_filter=args.histogram_filter)
