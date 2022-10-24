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
from util.filetree import FileTree

BASENAME = f"xp2.{datetime.now().isoformat(timespec='seconds')}"

LOCAL_FILETREE = FileTree().define([
    {"path": str(Path(__file__).parent), "tags": "root"},
    {"path": "@root/conf", "tags": "conf"},
    {"path": "@conf/cassandra", "tags": "cassandra-conf"},
    {"path": "@conf/driver", "tags": "driver-conf"},
    {"path": "@conf/workload", "tags": "workload-conf"},
    {"path": f"@root/input", "tags": "input"},
    {"path": f"@root/output", "tags": "output"},
])

DEFAULT_JOB_NAME = "cassandra"
DEFAULT_SITE = "nancy"
DEFAULT_CLUSTER = "gros"
DEFAULT_ENV_NAME = "debian11-x64-min"
DEFAULT_WALLTIME = "00:30:00"
DEFAULT_OUTPUT = str(LOCAL_FILETREE.path("output") / BASENAME)
DEFAULT_INFER_FROM = 0
DEFAULT_RAMPUP_RATE = 50_000
DEFAULT_REPORT_INTERVAL = 30
DEFAULT_HISTOGRAM_FILTER = ".*result:30s"
DEFAULT_DSTAT_OPTIONS = "-Tcmdrns -D total,sda5"

DSTAT_SLEEP_IN_SEC = 5

RAMPUP_MODE_ALWAYS = "always"
RAMPUP_MODE_KEEP_BETWEEN_SETS = "keep_between_sets"
RAMPUP_MODE_KEEP_BETWEEN_RUNS = "keep_between_runs"


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
    filetree = FileTree().define([
        {"path": output_path, "tags": ["root"]},
        {"path": "@root/raw", "tags": ["raw"]},
    ]).build()

    parameters.to_csv(filetree.path("root") / "input.all.csv")
    filtered_parameters.to_csv(filetree.path("root") / "input.csv")

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
        _key_size_in_bytes = params["key_size_in_bytes"]
        _value_size_in_bytes = params["value_size_in_bytes"]
        _bytes_per_host = params["bytes_per_host"]
        _rampup_mode = params["rampup_mode"]
        _docker_image = params["docker_image"]
        _config_file = params["config_file"]
        _driver_config_file = params["driver_config_file"]
        _workload_file = params["workload_file"]
        _workload_parameters = params["workload_parameters"]

        logging.info(f"Preparing {_name}#{_id}...")

        filetree.define([
            {"path": f"@raw/{_name}", "tags": ["set", _name]},
            {"path": f"@{_name}/conf", "tags": [f"{_name}__conf"]}
        ]).build()

        rf = min(_rf, _hosts)
        bytes_total = _hosts * _bytes_per_host / rf
        key_count = round(bytes_total / _value_size_in_bytes)
        ops_per_client = _ops / _clients

        workload_parameters = {}
        if not pd.isna(_workload_parameters):
            for parameter in _workload_parameters.split(","):
                _parameter = parameter.split("=")
                workload_parameters[_parameter[0]] = _parameter[1]

        # Infer saturating throughput
        sat_throughput = infer_throughput(parameters=parameters,
                                          ref_id=_throughput_ref,
                                          basepath=filetree.path("raw"),
                                          start_time=infer_from)

        # Compute real throughput on each client
        throughput_per_client = 0
        if sat_throughput > 0:
            logging.info(f"Saturating throughput currently set to {sat_throughput}.")

            throughput_per_client = _throughput * sat_throughput / _clients

        cassandra_hosts = resources.roles["cassandra"][:_hosts]
        nb_hosts = resources.roles["clients"][:_clients]

        nb_driver_config_file = LOCAL_FILETREE.path("driver-conf") / _driver_config_file
        nb_workload_file = LOCAL_FILETREE.path("workload-conf") / _workload_file

        # Save config
        filetree.copy([
            LOCAL_FILETREE.path("cassandra-conf") / _config_file,
            LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml",
            LOCAL_FILETREE.path("driver-conf") / _driver_config_file,
            LOCAL_FILETREE.path("workload-conf") / _workload_file
        ], f"{_name}__conf")

        # Save input parameters
        input_row = parameters[parameters.index == _id]
        input_row.to_csv(filetree.path(_name) / "input.csv")

        rampup_done["run"] = None
        for run_index in range(_repeat):
            filetree.define([
                {"path": f"@{_name}/run-{run_index}", "tags": [f"{_name}-{run_index}"]},
                {"path": f"@{_name}-{run_index}/tmp", "tags": [f"{_name}-{run_index}__tmp"]}
            ]).build()

            logging.info(f"Running {_name}#{_id} - run {run_index}.")

            should_rampup = (_rampup_mode == RAMPUP_MODE_ALWAYS
                             or (_rampup_mode == RAMPUP_MODE_KEEP_BETWEEN_SETS and rampup_done["set"] is None)
                             or (_rampup_mode == RAMPUP_MODE_KEEP_BETWEEN_RUNS and rampup_done["run"] is None))

            logging.info(f"rampup_mode={_rampup_mode},set={rampup_done['set']},run={rampup_done['run']};"
                         f" will{' not' if not should_rampup else ''} rampup.")

            # Deploy NoSQLBench
            nb = NoSQLBench(docker_image="adugois1/nosqlbench:latest")
            nb.deploy(nb_hosts)

            nb_driver = nb.filetree("remote_container").path("driver-conf") / nb_driver_config_file.name
            nb_workload = nb.filetree("remote_container").path("workload-conf") / nb_workload_file.name
            nb_data = nb.filetree("remote_container").path("data")

            # Deploy and start Cassandra
            cassandra = Cassandra(name="cassandra", docker_image=_docker_image)

            cassandra.init(cassandra_hosts, reset=should_rampup)
            cassandra.create_config(LOCAL_FILETREE.path("cassandra-conf") / _config_file)
            cassandra.create_extra_config([LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
                                           LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
                                           LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml"])
            cassandra.deploy_and_start()

            logging.info(cassandra.status())

            if should_rampup:
                logging.info("Executing rampup phase.")

                # Create schema
                schema_options = dict(driver="cqld4",
                                      workload=nb_workload,
                                      alias="xp2",
                                      tags="block:schema",
                                      driverconfig=nb_driver,
                                      threads=1,
                                      rf=rf,
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc="datacenter1")

                schema_cmd = RunCommand.from_options(**schema_options)

                nb.single_command(name="nb-schema",
                                  command=schema_cmd,
                                  driver_path=nb_driver_config_file,
                                  workload_path=nb_workload_file)

                # Insert data
                rampup_options = dict(driver="cqld4",
                                      workload=nb_workload,
                                      alias="xp2",
                                      tags="block:rampup",
                                      driverconfig=nb_driver,
                                      threads="auto",
                                      cycles=key_count,
                                      cyclerate=rampup_rate,
                                      stride=_stride,
                                      keysize=_key_size_in_bytes,
                                      valuesize=_value_size_in_bytes,
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc="datacenter1")

                for param_key in workload_parameters:
                    rampup_options[param_key] = workload_parameters[param_key]

                rampup_cmd = RunCommand.from_options(**rampup_options)

                nb.single_command(name="nb-rampup",
                                  command=rampup_cmd,
                                  driver_path=nb_driver_config_file,
                                  workload_path=nb_workload_file)

                # Flush memtable to SSTable
                cassandra.nodetool("flush -- baselines keyvalue")

                # Perform a major compaction
                cassandra.nodetool("compact")

                # Mark rampup done
                rampup_done["set"] = _id
                rampup_done["run"] = run_index

            logging.info(cassandra.tablestats("baselines.keyvalue"))
            logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

            # Main experiment
            main_options = dict(driver="cqld4",
                                workload=nb_workload,
                                alias="xp2",
                                tags="block:main-read",
                                driverconfig=nb_driver,
                                threads=_threads,
                                stride=_stride,
                                keycount=key_count,
                                keysize=_key_size_in_bytes,
                                errors="timer",
                                host=cassandra.get_host_address(0),
                                localdc="datacenter1")

            for param_key in workload_parameters:
                main_options[param_key] = workload_parameters[param_key]

            if throughput_per_client > 0:
                main_options["cyclerate"] = throughput_per_client

            main_cmds = []
            for index, host in enumerate(nb.hosts):
                start_cycle = int(index * ops_per_client)
                end_cycle = int(start_cycle + ops_per_client)

                main_options["cycles"] = f"{start_cycle}..{end_cycle}"

                main_cmd = RunCommand \
                    .from_options(**main_options) \
                    .logs_dir(nb_data) \
                    .log_histograms(nb_data / f"histograms.csv:{histogram_filter}") \
                    .log_histostats(nb_data / f"histostats.csv:{histogram_filter}") \
                    .report_summary_to(nb_data / "summary.txt") \
                    .report_csv_to(nb_data / "csv") \
                    .report_interval(report_interval)

                main_cmds.append((host, main_cmd))

            _tmp_dstat_path = filetree.path(f"{_name}-{run_index}__tmp") / "dstat"
            _tmp_data_path = filetree.path(f"{_name}-{run_index}__tmp") / "data"
            _tmp_metrics_path = filetree.path(f"{_name}-{run_index}__tmp") / "metrics"
            with en.Dstat(nodes=[*cassandra.hosts, *nb.hosts], options=dstat_options, backup_dir=_tmp_dstat_path):
                time.sleep(DSTAT_SLEEP_IN_SEC)  # Make sure Dstat is running when we start main experiment

                nb.command(name="nb-main",
                           commands=main_cmds,
                           driver_path=nb_driver_config_file,
                           workload_path=nb_workload_file)

                time.sleep(DSTAT_SLEEP_IN_SEC)  # Let the system recover before killing Dstat

            # Get NoSQLBench results
            nb.pull_results(_tmp_data_path)

            # Get Cassandra metrics
            cassandra.get_metrics(_tmp_metrics_path)

            # Save results
            _client_path = filetree.path(f"{_name}-{run_index}__tmp") / "clients"
            _client_path.mkdir(parents=True, exist_ok=True)

            _host_path = filetree.path(f"{_name}-{run_index}__tmp") / "hosts"
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

                _metrics_dir = _tmp_metrics_path / host.address / "metrics"
                if _metrics_dir.exists():
                    shutil.copytree(_metrics_dir, _host_path / host.address / "metrics")
                else:
                    logging.warning(f"{_metrics_dir} does not exist.")

            with (_host_path / "cassandra.log").open("w") as file:
                file.write(cassandra.logs())

            shutil.rmtree(_tmp_dstat_path)
            shutil.rmtree(_tmp_data_path)
            shutil.rmtree(_tmp_metrics_path)

            # logging.info(f"Results have been saved in {filetree.path(f"{_name}-{run_index}__tmp")}.")

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
