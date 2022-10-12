import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import enoslib as en
import pandas as pd

from drivers.Cassandra import Cassandra
from drivers.NoSQLBench import NoSQLBench, RunCommand
from drivers.Resources import Resources
from util.infer import MeanRateInference

ROOT = Path(__file__).parent

BASENAME = f"{ROOT.name}.{datetime.now().isoformat(timespec='seconds')}"

DEFAULT_JOB_NAME = "cassandra"
DEFAULT_SITE = "nancy"
DEFAULT_CLUSTER = "gros"
DEFAULT_ENV_NAME = "debian11-x64-min"
DEFAULT_WALLTIME = "00:30:00"
DEFAULT_OUTPUT = str(ROOT / "output" / BASENAME)
DEFAULT_INFER_FROM = 0
DEFAULT_RAMPUP_RATE = 50_000
DEFAULT_REPORT_INTERVAL = 30
DEFAULT_HISTOGRAM_FILTER = ".*result:30s"
DEFAULT_DSTAT_OPTIONS = "-Tcmdns"

DSTAT_SLEEP_IN_SEC = 5


def infer_throughput(parameters: pd.DataFrame, ref_id: str, basepath: Path, start_time: int):
    if not pd.isna(ref_id):
        rows = parameters[parameters.index == ref_id]
        if rows.empty:
            logging.error(f"Reference #{ref_id} does not exist. Could not infer saturating throughput.")
        else:
            ref_params = rows.iloc[0]
            ref_name = ref_params["name"]

            _ref_path = basepath / ref_name
            if _ref_path.exists():
                return MeanRateInference(_ref_path, start_time) \
                    .set_run_paths("run-*") \
                    .infer("**/*.result.csv")
            else:
                logging.warning(f"Reference #{ref_id} has not been executed. Could not infer saturating throughput.")

    return 0


def run(site: str,
        cluster: str,
        settings: dict,
        parameters: pd.DataFrame,
        filtered_parameters: pd.DataFrame,
        output_path: str,
        rampup_rate=DEFAULT_RAMPUP_RATE,
        infer_from=DEFAULT_INFER_FROM,
        report_interval=DEFAULT_REPORT_INTERVAL,
        histogram_filter=DEFAULT_HISTOGRAM_FILTER,
        dstat_options=DEFAULT_DSTAT_OPTIONS):
    """
    Launch experiment and produce results, organized as following file tree.

    output/
    ├─ xp1_baseline.2022-01-01T00:00:00/            <- Current experiment
    │  ├─ raw/                                      <- Raw results
    │  │  ├─ set-0/                                 <- First set of parameters
    │  │  │  ├─ config/                             <- Config files used in this experiment
    │  │  │  ├─ run-0/                              <- First run of this experiment
    │  │  │  │  ├─ clients/                         <- Data related to clients
    │  │  │  │  │  ├─ gros-1.nancy.grid5000.fr/     <- Data related to client gros-1
    │  │  │  │  │  │  ├─ data/                      <- NoSQLBench data
    │  │  │  │  │  │  ├─ dstat/                     <- Dstat data
    │  │  │  │  │  ├─ gros-2.nancy.grid5000.fr/
    │  │  │  │  │  │  ├─ ...
    │  │  │  │  ├─ hosts/
    │  │  │  │  │  ├─ ...
    │  │  │  ├─ run-1/
    │  │  │  │  ├─ ...
    │  │  │  ├─ run-2/
    │  │  │  │  ├─ ...
    │  │  ├─ set-1/
    │  │  │  ├─ ...
    │  │  ├─ set-2/
    │  │  │  ├─ ...
    │  ├─ input.all.csv
    │  ├─ input.csv
    """

    _output_path = Path(output_path)
    _output_path.mkdir(parents=True, exist_ok=True)

    _raw_path = _output_path / "raw"
    _raw_path.mkdir(parents=True, exist_ok=True)

    parameters.to_csv(_output_path / "input.all.csv")
    filtered_parameters.to_csv(_output_path / "input.csv")

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
    rampup_done = {"set": None, "run": None}
    for _id, params in filtered_parameters.iterrows():
        _name = params["name"]
        _repeat = params["repeat"]
        _version = params["version"]
        _hosts = params["hosts"]
        _clients = params["clients"]
        _threads = params["threads"]
        _stride = params["stride"]
        _ops = params["ops"]
        _throughput = params["throughput"]
        _throughput_ref = params["throughput_ref"]
        _rf = params["rf"]
        _value_size_in_bytes = params["value_size_in_bytes"]
        _bytes_per_host = params["bytes_per_host"]
        _rampup_mode = params["rampup_mode"]
        _docker_image = params["docker_image"]
        _config_file = params["config_file"]
        _driver_config_file = params["driver_config_file"]
        _workload_file = params["workload_file"]

        logging.info(f"Preparing {_name}#{_id}...")

        _set_path = _raw_path / _name
        _config_path = _set_path / "config"
        _config_path.mkdir(parents=True, exist_ok=True)

        rf = min(_rf, _hosts)
        bytes_total = _hosts * _bytes_per_host / rf
        key_count = round(bytes_total / _value_size_in_bytes)
        ops_per_client = _ops / _clients

        # Infer saturating throughput
        sat_throughput = infer_throughput(parameters=parameters,
                                          ref_id=_throughput_ref,
                                          basepath=_raw_path,
                                          start_time=infer_from)

        # Compute real throughput on each client
        throughput_per_client = 0
        if sat_throughput > 0:
            logging.info(f"Saturating throughput currently set to {sat_throughput}.")

            throughput_per_client = _throughput * sat_throughput / _clients

        cassandra_hosts = resources.roles["cassandra"][:_hosts]
        nb_hosts = resources.roles["clients"][:_clients]

        cassandra_config_path = ROOT / "config" / "cassandra"
        nb_driver_path = ROOT / "config" / "driver"
        nb_workload_path = ROOT / "config" / "workloads"

        cassandra_config_file = cassandra_config_path / _config_file
        nb_driver_config_file = nb_driver_path / _driver_config_file
        nb_workload_file = nb_workload_path / _workload_file

        # Save config
        shutil.copy2(cassandra_config_file, _config_path / "cassandra.yaml")
        shutil.copy2(cassandra_config_path / "jvm-server.options", _config_path)
        shutil.copy2(cassandra_config_path / "jvm11-server.options", _config_path)
        shutil.copy2(nb_driver_config_file, _config_path / "nb-driver.conf")
        shutil.copy2(nb_workload_file, _config_path / "nb-workload.yaml")

        # Save input parameters
        input_row = parameters[parameters.index == _id]
        input_row.to_csv(_set_path / "input.csv")

        rampup_done["run"] = None
        for run_index in range(_repeat):
            _run_path = _set_path / f"run-{run_index}"
            _run_path.mkdir(parents=True, exist_ok=True)

            logging.info(f"Running {_name}#{_id} - run {run_index}.")

            should_rampup = (_rampup_mode == "always"
                             or (_rampup_mode == "keep_set" and rampup_done["set"] is None)
                             or (_rampup_mode == "keep_run" and rampup_done["run"] is None))

            logging.info(f"rampup_mode={_rampup_mode},set={rampup_done['set']},run={rampup_done['run']};"
                         f" will{' not' if not should_rampup else ''} rampup.")

            # Deploy NoSQLBench
            nb = NoSQLBench(name="nb",
                            docker_image="nosqlbench/nosqlbench:nb5preview",
                            driver_path=nb_driver_path,
                            workload_path=nb_workload_path)

            nb.init(nb_hosts)
            nb.deploy()

            # Deploy and start Cassandra
            cassandra = Cassandra(name="cassandra", docker_image=_docker_image)

            cassandra.init(cassandra_hosts, reset=should_rampup)
            cassandra.create_config(cassandra_config_file)
            cassandra.create_extra_config([cassandra_config_path / "jvm-server.options",
                                           cassandra_config_path / "jvm11-server.options"])
            cassandra.deploy_and_start()

            logging.info(cassandra.status())

            if should_rampup:
                logging.info("Executing rampup phase.")

                # Create schema
                schema_options = dict(driver="cqld4",
                                      workload=nb.workload(nb_workload_file.name),
                                      alias=ROOT.name,
                                      tags="block:schema",
                                      driverconfig=nb.driver(nb_driver_config_file.name),
                                      threads=1,
                                      rf=rf,
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc="datacenter1")

                schema_cmd = RunCommand.from_options(**schema_options)

                nb.single_command("nb-schema", schema_cmd)

                # Insert data
                rampup_options = dict(driver="cqld4",
                                      workload=nb.workload(nb_workload_file.name),
                                      alias=ROOT.name,
                                      tags="block:rampup",
                                      driverconfig=nb.driver(nb_driver_config_file.name),
                                      threads="auto",
                                      cycles=key_count,
                                      cyclerate=rampup_rate,
                                      stride=_stride,
                                      keycount=key_count,
                                      valuesize=_value_size_in_bytes,
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc="datacenter1")

                rampup_cmd = RunCommand.from_options(**rampup_options)

                nb.single_command("nb-rampup", rampup_cmd)

                # Flush memtable to SSTable
                cassandra.nodetool("flush -- baselines keyvalue")

                # Mark rampup done
                rampup_done["set"] = _id
                rampup_done["run"] = run_index

            logging.info(cassandra.tablestats("baselines.keyvalue"))
            logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

            # Main experiment
            main_options = dict(driver="cqld4",
                                workload=nb.workload(nb_workload_file.name),
                                alias=ROOT.name,
                                tags="block:main-read",
                                driverconfig=nb.driver(nb_driver_config_file.name),
                                threads=_threads,
                                stride=_stride,
                                keycount=key_count,
                                errors="timer",
                                host=cassandra.get_host_address(0),
                                localdc="datacenter1")

            if throughput_per_client > 0:
                main_options["cyclerate"] = throughput_per_client

            main_cmds = []
            for index, host in enumerate(nb.hosts):
                start_cycle = int(index * ops_per_client)
                end_cycle = int(start_cycle + ops_per_client)

                main_options["cycles"] = f"{start_cycle}..{end_cycle}"

                main_cmd = RunCommand \
                    .from_options(**main_options) \
                    .logs_dir(nb.data()) \
                    .log_histograms(nb.data(f"histograms.csv:{histogram_filter}")) \
                    .log_histostats(nb.data(f"histostats.csv:{histogram_filter}")) \
                    .report_summary_to(nb.data("summary.txt")) \
                    .report_csv_to(nb.data("csv")) \
                    .report_interval(report_interval)

                main_cmds.append((host, main_cmd))

            _tmp_dstat_path = _run_path / "dstat"
            _tmp_data_path = _run_path / "data"
            with en.Dstat(nodes=[*cassandra.hosts, *nb.hosts], options=dstat_options, backup_dir=_tmp_dstat_path):
                time.sleep(DSTAT_SLEEP_IN_SEC)  # Make sure Dstat is running when we start main experiment
                nb.command("nb-main", main_cmds)
                time.sleep(DSTAT_SLEEP_IN_SEC)  # Let the system recover before killing Dstat

            nb.sync_results(_tmp_data_path)

            # Save results
            _client_path = _run_path / "clients"
            _client_path.mkdir(parents=True, exist_ok=True)

            _host_path = _run_path / "hosts"
            _host_path.mkdir(parents=True, exist_ok=True)

            for client in nb.hosts:
                _dstat_dir = _tmp_dstat_path / client.address
                if _dstat_dir.exists():
                    _client_dstat_path = _client_path / client.address / "dstat"
                    _client_dstat_path.mkdir(parents=True, exist_ok=True)

                    for _dstat_file in _dstat_dir.glob("**/*-dstat.csv"):
                        shutil.copy2(_dstat_file, _client_dstat_path / _dstat_file.name)
                else:
                    logging.warning(f"{_dstat_dir} does not exist.")

                _data_dir = _tmp_data_path / client.address / "data"
                if _data_dir.exists():
                    shutil.copytree(_data_dir, _client_path / client.address / "data")
                else:
                    logging.warning(f"{_data_dir} does not exist.")

            for host in cassandra.hosts:
                _dstat_dir = _tmp_dstat_path / host.address
                if _dstat_dir.exists():
                    _host_dstat_path = _host_path / host.address / "dstat"
                    _host_dstat_path.mkdir(parents=True, exist_ok=True)

                    for _dstat_file in _dstat_dir.glob("**/*-dstat.csv"):
                        shutil.copy2(_dstat_file, _host_dstat_path / _dstat_file.name)
                else:
                    logging.warning(f"{_dstat_dir} does not exist.")

            with (_host_path / "cassandra.log").open("w") as file:
                file.write(cassandra.logs())

            shutil.rmtree(_tmp_dstat_path)
            shutil.rmtree(_tmp_data_path)

            logging.info(f"Results have been saved in {_run_path}.")

            # Destroy instances
            logging.info("Destroying instances.")

            nb.destroy()
            cassandra.destroy()

    # Release resources
    resources.release()


if __name__ == "__main__":
    import argparse

    from sys import stdout
    from enoslib.config import set_config

    set_config(ansible_stdout="noop")

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
    parser.add_argument("--infer-from", type=int, default=DEFAULT_INFER_FROM)
    parser.add_argument("--report-interval", type=int, default=DEFAULT_REPORT_INTERVAL)
    parser.add_argument("--histogram-filter", type=str, default=DEFAULT_HISTOGRAM_FILTER)
    parser.add_argument("--id", type=str, action="append", default=None)
    parser.add_argument("--from-id", type=str, default=None)
    parser.add_argument("--to-id", type=str, default=None)
    parser.add_argument("--log", type=str, default=None)

    args = parser.parse_args()

    log_options = dict(level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")
    if args.log is not None:
        log_path = Path(args.log)
        log_path.mkdir(parents=True, exist_ok=True)

        log_options["filename"] = str(log_path / f"{BASENAME}.log")
    else:
        log_options["stream"] = stdout

    logging.basicConfig(**log_options)

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
        infer_from=args.infer_from,
        report_interval=args.report_interval,
        histogram_filter=args.histogram_filter)
