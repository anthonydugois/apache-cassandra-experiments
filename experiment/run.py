import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import enoslib as en
import pandas as pd

from .drivers import CassandraDriver, NBDriver, RunCommand, StartCommand, AwaitCommand, StopCommand, Scenario
from .resources import G5kResources
from .util import FileTree, Infer, CSVInput

LOCAL_FILETREE = FileTree().define([
    {"path": str(Path(__file__).parent), "tags": ["root"]},
    {"path": "@root/conf", "tags": ["conf"]},
    {"path": "@conf/cassandra", "tags": ["cassandra-conf"]},
    {"path": "@conf/driver", "tags": ["driver-conf"]},
    {"path": "@conf/workload", "tags": ["workload-conf"]},
    {"path": f"@root/input", "tags": ["input"]},
    {"path": f"@root/output", "tags": ["output"]},
])

DSTAT_SLEEP_IN_SEC = 5
RUN_SLEEP_IN_SEC = 120

MIN_RATE_LIMIT = 100.0


class RateLimitFormatException(Exception):
    pass


def rate_limit_from_expr(expr: str, csv_input: CSVInput, basepath: Path):
    if pd.isna(expr) or expr.startswith("none"):
        return "none", 0.0
    elif expr.startswith("infer="):
        return "infer", Infer(csv_input, basepath).infer_from_expr(expr.split("=")[1])
    elif expr.startswith("linear="):
        return "linear", float(expr.split("=")[1])
    elif expr.startswith("fixed="):
        return "fixed", float(expr.split("=")[1])
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
    csv_input.view("input").to_csv(output_ft.path("root") / "input.csv")

    # Warning: the two following values must be wrapped in an int, as pandas returns an np.int64,
    # which is not usable in the resource driver.
    max_hosts = int(csv_input.view(key="input", columns="hosts").max())
    max_clients = int(csv_input.view(key="input", columns="clients").max())

    # Acquire G5k resources
    resources = G5kResources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["nodes", "cassandra"], max_hosts)
    resources.add_machines(["nodes", "clients"], max_clients)

    resources.acquire(with_docker="nodes")

    # Run experiments
    for _id, params in csv_input.view("input").iterrows():
        _name = params["name"]
        _repeat = params["repeat"]
        _hosts = params["hosts"]
        _rf = params["rf"]
        _read_ratio = params["read_ratio"]
        _write_ratio = params["write_ratio"]
        _keys = params["keys"]
        _ops = params["ops"]
        _rampup_rate_limit = params["rampup_rate_limit"]
        _main_rate_limit = params["main_rate_limit"]
        _warmup_rate_limit = params["warmup_rate_limit"]
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
            {"path": "@root/conf", "tags": ["conf"]},
            {"path": "@root/data", "tags": ["data"]}
        ]).build()

        ops_per_client = _ops / _clients
        rampup_rate_type, rampup_rate_limit = rate_limit_from_expr(_rampup_rate_limit, csv_input, output_ft.path("raw"))
        main_rate_type, main_rate_limit = rate_limit_from_expr(_main_rate_limit, csv_input, output_ft.path("raw"))
        warmup_rate_type, warmup_rate_limit = rate_limit_from_expr(_warmup_rate_limit, csv_input, output_ft.path("raw"))

        cassandra_hosts = list(resources.roles["cassandra"][:_hosts])
        nb_hosts = list(resources.roles["clients"][:_clients])

        nb_driver_config_path = LOCAL_FILETREE.path("driver-conf") / _driver_config_file
        nb_workload_config_path = LOCAL_FILETREE.path("workload-conf") / _workload_config_file

        # Save config files
        config_files = [
            LOCAL_FILETREE.path("cassandra-conf") / _config_file,
            LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml",
            LOCAL_FILETREE.path("driver-conf") / _driver_config_file,
            LOCAL_FILETREE.path("workload-conf") / _workload_config_file
        ]

        set_output_ft.copy(config_files, "conf")

        # Save input parameters
        input_path = set_output_ft.path("root") / "input.csv"
        csv_input.filter([_id]).to_csv(input_path)

        # Deploy NoSQLBench
        nb = NBDriver(docker_image="adugois1/nosqlbench:latest")

        nb.deploy(nb_hosts)
        nb.filetree("remote").copy([nb_driver_config_path], tag="driver-conf", remote=nb_hosts)
        nb.filetree("remote").copy([nb_workload_config_path], tag="workload-conf", remote=nb_hosts)

        nb_driver_config = nb.filetree("remote_container").path("driver-conf") / nb_driver_config_path.name
        nb_workload_config = nb.filetree("remote_container").path("workload-conf") / nb_workload_config_path.name
        nb_data_path = nb.filetree("remote_container").path("data")

        # Deploy and start Cassandra
        cassandra = CassandraDriver(docker_image=_docker_image)

        cassandra.init(cassandra_hosts, reset=True)
        cassandra.create_config(LOCAL_FILETREE.path("cassandra-conf") / _config_file)
        cassandra.create_extra_config([LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
                                       LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
                                       LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml"])

        cassandra.deploy().start().cleanup()

        logging.info(cassandra.status())

        # Rampup
        logging.info("Executing rampup phase.")

        nb.command(
            Scenario.create(
                RunCommand.create(**{
                    "alias": "schema",
                    "driver": "cqld4",
                    "driverconfig": nb_driver_config,
                    "workload": nb_workload_config,
                    "tags": "block:schema",
                    "threads": 1,
                    "errors": "warn,retry",
                    "host": cassandra.get_host_address(0),
                    "localdc": "datacenter1",
                    "rf": int(_rf)
                }),
                RunCommand.create(**{
                    "alias": "rampup",
                    "driver": "cqld4",
                    "driverconfig": nb_driver_config,
                    "workload": nb_workload_config,
                    "tags": "block:rampup",
                    "threads": "auto",
                    "cyclerate": rampup_rate_limit,
                    "cycles": f"1..{int(_keys) + 1}",
                    "stride": int(_client_stride),
                    "errors": "warn,retry",
                    "host": cassandra.get_host_address(0),
                    "localdc": "datacenter1",
                    "keysize": int(_key_size),
                    "valuesizedist": f"'{_value_size_dist}'"
                })
            )
            .as_string()
        )

        # Flush memtable to SSTable
        # cassandra.flush("baselines", "keyvalue")

        logging.info(cassandra.tablestats("baselines", "keyvalue"))

        # The very first run (index 0) is a warmup phase.
        # That's why we have one additional iteration here.
        for run_index in range(_repeat + 1):
            logging.info(f"Waiting for the system before running run {run_index}...")

            time.sleep(RUN_SLEEP_IN_SEC)

            logging.info(f"Running {_name}#{_id} - run {run_index}.")

            run_output_ft = FileTree().define([
                {"path": str(set_output_ft.path("root") / f"run-{run_index}"), "tags": ["root"]},
                {"path": "@root/tmp", "tags": ["tmp"]},
                {"path": "@tmp/dstat", "tags": ["dstat"]},
                {"path": "@tmp/data", "tags": ["data"]},
                {"path": "@root/clients", "tags": ["clients"]},
                {"path": "@root/hosts", "tags": ["hosts"]}
            ]).build()

            if run_index <= 0:
                main_rate_limit_per_client = warmup_rate_limit / _clients
            else:
                if main_rate_type == "linear":
                    main_rate_limit_per_client = (run_index * main_rate_limit) / _clients
                else:
                    main_rate_limit_per_client = main_rate_limit / _clients

            rw_total = _read_ratio + _write_ratio
            read_ratio = _read_ratio / rw_total
            write_ratio = _write_ratio / rw_total

            read_ops_per_client = int(read_ratio * ops_per_client)
            write_ops_per_client = int(write_ratio * ops_per_client)

            read_threads = int(read_ratio * _client_threads)
            write_threads = int(write_ratio * _client_threads)

            read_params = {
                "alias": "read",
                "driver": "cqld4",
                "driverconfig": nb_driver_config,
                "workload": nb_workload_config,
                "tags": "block:main-read",
                "threads": read_threads,
                "stride": int(_client_stride),
                "errors": "warn,timer",
                "host": cassandra.get_host_address(0),
                "localdc": "datacenter1",
                "keydist": f"'{_key_dist}'",
                "keysize": int(_key_size),
                "valuesizedist": f"'{_value_size_dist}'"
            }

            write_params = {
                "alias": "write",
                "driver": "cqld4",
                "driverconfig": nb_driver_config,
                "workload": nb_workload_config,
                "tags": "block:main-write",
                "threads": write_threads,
                "stride": int(_client_stride),
                "errors": "warn,timer",
                "host": cassandra.get_host_address(0),
                "localdc": "datacenter1",
                "keydist": f"'{_key_dist}'",
                "keysize": int(_key_size),
                "valuesizedist": f"'{_value_size_dist}'"
            }

            if main_rate_limit_per_client >= MIN_RATE_LIMIT:
                estimated_duration = read_ops_per_client / main_rate_limit_per_client
                read_params["cyclerate"] = main_rate_limit_per_client
                write_params["cyclerate"] = write_ops_per_client / estimated_duration

                logging.info(f"Estimated duration: {estimated_duration} seconds.")

            main_cmds = []
            for index, host in enumerate(nb.hosts):
                read_start = int(index * read_ops_per_client)
                read_end = int(read_start + read_ops_per_client)
                read_cycles = f"{read_start}..{read_end}"

                write_start = int(index * write_ops_per_client)
                write_end = int(write_start + write_ops_per_client)
                write_cycles = f"{write_start}..{write_end}"

                if _write_ratio > 0:
                    commands = [
                        StartCommand.create(**read_params, cycles=read_cycles),
                        StartCommand.create(**write_params, cycles=write_cycles),
                        AwaitCommand.create("read"),
                        StopCommand.create("write")
                    ]
                else:
                    commands = [
                        StartCommand.create(**read_params, cycles=read_cycles),
                        AwaitCommand.create("read")
                    ]

                main_cmds.append((
                    host,
                    Scenario.create(*commands)
                    .logs_dir(nb_data_path)
                    .log_histograms(nb_data_path / f"histograms.csv:{histogram_filter}")
                    .log_histostats(nb_data_path / f"histostats.csv:{histogram_filter}")
                    .report_summary_to(nb_data_path / "summary.txt")
                    .report_csv_to(nb_data_path / "csv")
                    .report_interval(report_interval)
                    .as_string()
                ))

            _tmp_dstat_path = run_output_ft.path("dstat")
            _tmp_data_path = run_output_ft.path("data")

            with en.Dstat(nodes=[*cassandra.hosts, *nb.hosts], options=dstat_options, backup_dir=_tmp_dstat_path):
                # Make sure Dstat is running when we start experiment
                time.sleep(DSTAT_SLEEP_IN_SEC)

                # Launch main commands
                nb.commands(main_cmds)

                # Let the system recover before killing Dstat
                time.sleep(DSTAT_SLEEP_IN_SEC)

            # Get NoSQLBench results
            nb.pull_results(_tmp_data_path)

            # Get Cassandra metrics
            # cassandra.pull_metrics(_tmp_log_path)

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

                # _metrics_dir = _tmp_log_path / host.address / "metrics"
                # if _metrics_dir.exists():
                #     shutil.copytree(_metrics_dir, _host_path / host.address / "metrics")
                # else:
                #     logging.warning(f"{_metrics_dir} does not exist.")
                #
                # _log_dir = _tmp_log_path / host.address / "log"
                # if _log_dir.exists():
                #     shutil.copytree(_log_dir, _host_path / host.address / "log")
                # else:
                #     logging.warning(f"{_log_dir} does not exist.")

            run_output_ft.remove("tmp")

        # Pull Cassandra logs
        cassandra.pull_log(set_output_ft.path("data"))

        # Shutdown and destroy
        logging.info("Destroying instances.")

        nb.destroy()
        cassandra.shutdown().destroy()

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
    DEFAULT_REPORT_INTERVAL = 10
    DEFAULT_HISTOGRAM_FILTER = "read.cycles.servicetime:10s"

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

    csv_input = CSVInput(args.input)
    csv_input.create_view("input", csv_input.get_ids(from_id=args.from_id, to_id=args.to_id, ids=args.id))

    settings = dict(job_name=args.job_name, env_name=args.env_name, walltime=args.walltime)
    if args.reservation is not None:
        settings["reservation"] = args.reservation

    if args.output is None:
        now = datetime.now().isoformat(timespec='seconds')
        output_path = LOCAL_FILETREE.path("output") / f"{args.job_name}.{now}"
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

    run(site=args.site, cluster=args.cluster, settings=settings, csv_input=csv_input, output_path=output_path,
        report_interval=args.report_interval, histogram_filter=args.histogram_filter)
