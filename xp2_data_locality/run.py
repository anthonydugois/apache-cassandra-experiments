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
from util.filetree import FileTree
from util.infer import Infer
from util.input import CSVInput

LOCAL_FILETREE = FileTree().define([
    {"path": str(Path(__file__).parent), "tags": ["root"]},
    {"path": "@root/conf", "tags": ["conf"]},
    {"path": "@conf/cassandra", "tags": ["cassandra-conf"]},
    {"path": "@conf/driver", "tags": ["driver-conf"]},
    {"path": "@conf/workload", "tags": ["workload-conf"]},
    {"path": f"@root/input", "tags": ["input"]},
    {"path": f"@root/output", "tags": ["output"]},
])

NB_ALIAS = "xp"
NB_DRIVER = "cqld4"
NB_DRIVER_LOCALDC = "datacenter1"
NB_BLOCK_CSV_FREQS = "block:csv-freqs"
NB_BLOCK_CSV_SIZES = "block:csv-sizes"
NB_BLOCK_SCHEMA = "block:schema"
NB_BLOCK_RAMPUP = "block:rampup"
NB_BLOCK_MAIN = "block:main.*"

DSTAT_SLEEP_IN_SEC = 5

MIN_RATE_LIMIT = 100.0


class RateLimitFormatException(Exception):
    pass


def rate_limit_from_expr(expr: str, csv_input: CSVInput, basepath: Path):
    if pd.isna(expr):
        return 0.0
    elif expr.startswith("infer="):
        return Infer(csv_input, basepath).infer_from_expr(expr.split("=")[1])
    elif expr.startswith("fixed="):
        return float(expr.split("=")[1])
    else:
        raise RateLimitFormatException


def run(site: str,
        cluster: str,
        settings: dict,
        csv_input: CSVInput,
        output_path: Path,
        report_interval: int,
        histogram_filter: str,
        dstat_options="-Tcmdrns -D total,sda5"):
    output_ft = FileTree().define([
        {"path": str(output_path), "tags": ["root"]},
        {"path": "@root/raw", "tags": ["raw"]},
    ]).build()

    csv_input.view().to_csv(output_ft.path("root") / "input.all.csv")
    csv_input.view(key="input").to_csv(output_ft.path("root") / "input.csv")

    # Warning: the two following values must be wrapped in an int, as pandas returns an np.int64,
    # which is not usable in the resource driver.
    max_hosts = int(csv_input.view(key="input", columns="hosts").max())
    max_clients = int(csv_input.view(key="input", columns="clients").max())

    # Acquire G5k resources
    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["nodes", "cassandra"], max_hosts)
    resources.add_machines(["nodes", "clients"], max_clients)

    resources.acquire(with_docker="nodes")

    # Run experiments
    for _id, params in csv_input.view("input").iterrows():
        _name = params["name"]
        _repeat = params["repeat"]
        _rampup_phase = params["rampup_phase"]
        _main_phase = params["main_phase"]
        _hosts = params["hosts"]
        _rf = params["rf"]
        _read_ratio = params["read_ratio"]
        _write_ratio = params["write_ratio"]
        _keys = params["keys"]
        _ops = params["ops"]
        _rampup_rate_limit = params["rampup_rate_limit"]
        _main_rate_limit = params["main_rate_limit"]
        _key_dist = params["key_dist"]
        _key_size = params["key_size"]
        _value_size_dist = params["value_size_dist"]
        _docker_image = params["docker_image"]
        _config_file = params["config_file"]
        _driver_config_file = params["driver_config_file"]
        _workload_config_file = params["workload_config_file"]
        _clients = params["clients"]
        _client_threads = params["client_threads"]
        _client_stride = params["client_stride"]

        logging.info(f"Preparing {_name}#{_id}...")

        set_output_ft = FileTree().define([
            {"path": str(output_ft.path("raw") / _name), "tags": ["root"]},
            {"path": "@root/conf", "tags": ["conf"]}
        ]).build()

        execute_rampup = _rampup_phase == "yes"
        execute_main = _main_phase == "yes"
        ops_per_client = _ops / _clients
        rampup_rate_limit = rate_limit_from_expr(_rampup_rate_limit, csv_input, output_ft.path("raw"))
        main_rate_limit = rate_limit_from_expr(_main_rate_limit, csv_input, output_ft.path("raw"))
        main_rate_limit_per_client = main_rate_limit / _clients

        cassandra_hosts = resources.roles["cassandra"][:_hosts]
        nb_hosts = resources.roles["clients"][:_clients]

        nb_driver_config_file = LOCAL_FILETREE.path("driver-conf") / _driver_config_file
        nb_workload_file = LOCAL_FILETREE.path("workload-conf") / _workload_config_file

        # Save config
        set_output_ft.copy([
            LOCAL_FILETREE.path("cassandra-conf") / _config_file,
            LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml",
            LOCAL_FILETREE.path("driver-conf") / _driver_config_file,
            LOCAL_FILETREE.path("workload-conf") / _workload_config_file
        ], "conf")

        # Save input parameters
        csv_input.filter([_id]).to_csv(set_output_ft.path("root") / "input.csv")

        for run_index in range(_repeat):
            logging.info(f"Running {_name}#{_id} - run {run_index}.")

            # Deploy NoSQLBench
            nb = NoSQLBench(docker_image="adugois1/nosqlbench:latest")
            nb.deploy(nb_hosts)

            nb_driver_config = nb.filetree("remote_container").path("driver-conf") / nb_driver_config_file.name
            nb_workload_config = nb.filetree("remote_container").path("workload-conf") / nb_workload_file.name
            nb_data_path = nb.filetree("remote_container").path("data")

            # Deploy and start Cassandra
            cassandra = Cassandra(name="cassandra", docker_image=_docker_image)

            cassandra.init(cassandra_hosts, reset=execute_rampup)
            cassandra.create_config(LOCAL_FILETREE.path("cassandra-conf") / _config_file)
            cassandra.create_extra_config([LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
                                           LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
                                           LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml"])
            cassandra.deploy_and_start()

            logging.info(cassandra.status())

            if execute_rampup:
                logging.info("Executing rampup phase.")

                # Create schema
                schema_options = dict(driver=NB_DRIVER,
                                      driverconfig=nb_driver_config,
                                      workload=nb_workload_config,
                                      alias=NB_ALIAS,
                                      tags=NB_BLOCK_SCHEMA,
                                      threads=1,
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc=NB_DRIVER_LOCALDC,
                                      rf=int(_rf))

                schema_cmd = RunCommand.from_options(**schema_options)

                nb.single_command(name="nb-schema",
                                  command=schema_cmd,
                                  driver_path=nb_driver_config_file,
                                  workload_path=nb_workload_file)

                # Insert data
                rampup_options = dict(driver=NB_DRIVER,
                                      driverconfig=nb_driver_config,
                                      workload=nb_workload_config,
                                      alias=NB_ALIAS,
                                      tags=NB_BLOCK_RAMPUP,
                                      threads="auto",
                                      cyclerate=rampup_rate_limit,
                                      cycles=f"1..{int(_keys) + 1}",
                                      stride=int(_client_stride),
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc=NB_DRIVER_LOCALDC,
                                      keysize=int(_key_size),
                                      valuesizedist=_value_size_dist)

                rampup_cmd = RunCommand.from_options(**rampup_options)

                nb.single_command(name="nb-rampup",
                                  command=rampup_cmd,
                                  driver_path=nb_driver_config_file,
                                  workload_path=nb_workload_file)

                # Flush memtable to SSTable
                cassandra.flush("baselines", "keyvalue")

            logging.info(cassandra.tablestats("baselines", "keyvalue"))
            logging.info(cassandra.du("{{remote_container_data_path}}/data/baselines"))

            if execute_main:
                logging.info("Executing main phase.")

                run_output_ft = FileTree().define([
                    {"path": str(set_output_ft.path("root") / f"run-{run_index}"), "tags": ["root"]},
                    {"path": "@root/tmp", "tags": ["tmp"]},
                    {"path": "@tmp/dstat", "tags": ["dstat"]},
                    {"path": "@tmp/data", "tags": ["data"]},
                    {"path": "@tmp/metrics", "tags": ["metrics"]},
                    {"path": "@root/clients", "tags": ["clients"]},
                    {"path": "@root/hosts", "tags": ["hosts"]}
                ]).build()

                main_options = dict(driver=NB_DRIVER,
                                    driverconfig=nb_driver_config,
                                    workload=nb_workload_config,
                                    alias=NB_ALIAS,
                                    tags=NB_BLOCK_MAIN,
                                    threads=_client_threads,
                                    stride=_client_stride,
                                    errors="timer",
                                    host=cassandra.get_host_address(0),
                                    localdc=NB_DRIVER_LOCALDC,
                                    keydist=_key_dist,
                                    keysize=int(_key_size),
                                    valuesizedist=_value_size_dist,
                                    readratio=int(_read_ratio),
                                    writeratio=int(_write_ratio))

                if main_rate_limit_per_client >= MIN_RATE_LIMIT:
                    main_options["cyclerate"] = main_rate_limit_per_client

                main_cmds = []
                for index, host in enumerate(nb.hosts):
                    start_cycle = int(index * ops_per_client)
                    end_cycle = int(start_cycle + ops_per_client)

                    main_options["cycles"] = f"{start_cycle}..{end_cycle}"

                    main_cmd = RunCommand \
                        .from_options(**main_options) \
                        .logs_dir(nb_data_path) \
                        .log_histograms(nb_data_path / f"histograms.csv:{histogram_filter}") \
                        .log_histostats(nb_data_path / f"histostats.csv:{histogram_filter}") \
                        .report_summary_to(nb_data_path / "summary.txt") \
                        .report_csv_to(nb_data_path / "csv") \
                        .report_interval(report_interval)

                    main_cmds.append((host, main_cmd))

                _tmp_dstat_path = run_output_ft.path("dstat")
                _tmp_data_path = run_output_ft.path("data")
                _tmp_metrics_path = run_output_ft.path("metrics")

                with en.Dstat(nodes=[*cassandra.hosts, *nb.hosts],
                              options=dstat_options,
                              backup_dir=_tmp_dstat_path):
                    time.sleep(DSTAT_SLEEP_IN_SEC)  # Make sure Dstat is running when we start experiment

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
                _client_path = run_output_ft.path("clients")
                _host_path = run_output_ft.path("hosts")

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

                run_output_ft.remove("tmp")

            logging.info("Destroying instances.")

            nb.destroy()
            cassandra.destroy()

    # Release resources
    resources.release()


if __name__ == "__main__":
    import argparse

    from sys import stdout
    from enoslib.config import set_config

    DEFAULT_JOB_NAME = "cassandra"
    DEFAULT_SITE = "nancy"
    DEFAULT_CLUSTER = "gros"
    DEFAULT_ENV_NAME = "debian11-x64-min"
    DEFAULT_WALLTIME = "00:30:00"
    DEFAULT_REPORT_INTERVAL = 30
    DEFAULT_HISTOGRAM_FILTER = ".*result:30s"

    set_config(ansible_stdout="noop")

    parser = argparse.ArgumentParser()

    parser.add_argument("input", type=str, nargs="+")
    parser.add_argument("--job-name", type=str, default=DEFAULT_JOB_NAME)
    parser.add_argument("--site", type=str, default=DEFAULT_SITE)
    parser.add_argument("--cluster", type=str, default=DEFAULT_CLUSTER)
    parser.add_argument("--env-name", type=str, default=DEFAULT_ENV_NAME)
    parser.add_argument("--reservation", type=str, default=None)
    parser.add_argument("--walltime", type=str, default=DEFAULT_WALLTIME)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--report-interval", type=int, default=DEFAULT_REPORT_INTERVAL)
    parser.add_argument("--histogram-filter", type=str, default=DEFAULT_HISTOGRAM_FILTER)
    parser.add_argument("--id", type=str, nargs="*", default=None)
    parser.add_argument("--from-id", type=str, default=None)
    parser.add_argument("--to-id", type=str, default=None)
    parser.add_argument("--log", type=str, default=None)

    args = parser.parse_args()

    csv_input = CSVInput([Path(_input) for _input in args.input])
    csv_input.create_view("input", csv_input.get_ids(from_id=args.from_id, to_id=args.to_id, ids=args.id))

    settings = dict(job_name=args.job_name, env_name=args.env_name, walltime=args.walltime)

    if args.reservation is not None:
        settings["reservation"] = args.reservation

    if args.output is None:
        now = datetime.now().isoformat(timespec='seconds')
        output_path = LOCAL_FILETREE.path("output") / f"{csv_input.file_paths[0].stem}.{now}"
    else:
        output_path = Path(args.output)

    log_options = dict(level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")
    if args.log is not None:
        log_path = Path(args.log)
        log_path.mkdir(parents=True, exist_ok=True)
        log_options["filename"] = str(log_path / f"{output_path.name}.log")
    else:
        log_options["stream"] = stdout

    logging.basicConfig(**log_options)

    run(site=args.site,
        cluster=args.cluster,
        settings=settings,
        csv_input=csv_input,
        output_path=output_path,
        report_interval=args.report_interval,
        histogram_filter=args.histogram_filter)
