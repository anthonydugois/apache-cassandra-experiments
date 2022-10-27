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

DSTAT_SLEEP_IN_SEC = 5


def run(site: str,
        cluster: str,
        settings: dict,
        csv_input: CSVInput,
        output_path: Path,
        report_interval: int,
        histogram_filter: str,
        dstat_options="-Tcmdrns -D total,sda5"):
    filetree = FileTree().define([
        {"path": str(output_path), "tags": ["root"]},
        {"path": "@root/raw", "tags": ["raw"]},
    ]).build()

    csv_input.view().to_csv(filetree.path("root") / "input.all.csv")
    csv_input.view(key="filtered_sets").to_csv(filetree.path("root") / "input.csv")

    # Warning: the two following values must be wrapped in an int, as pandas returns an np.int64,
    # which is not usable in the resource driver.
    max_hosts = int(csv_input.view(key="filtered_sets", columns=["hosts"]).max())
    max_clients = int(csv_input.view(key="filtered_sets", columns=["clients"]).max())

    # Acquire G5k resources
    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["nodes", "cassandra"], max_hosts)
    resources.add_machines(["nodes", "clients"], max_clients)

    resources.acquire(with_docker="nodes")

    # Run experiments
    for _id, params in csv_input.view("filtered_sets").iterrows():
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
        _key_size_in_bytes = params["key_size_in_bytes"]
        _value_size_in_bytes = params["value_size_in_bytes"]
        _docker_image = params["docker_image"]
        _config_file = params["config_file"]
        _driver_config_file = params["driver_config_file"]
        _workload_config_file = params["workload_config_file"]
        _clients = params["clients"]
        _client_threads = params["client_threads"]
        _client_stride = params["client_stride"]

        logging.info(f"Preparing {_name}#{_id}...")

        filetree.define([
            {"path": f"@raw/{_name}", "tags": ["set", _name]},
            {"path": f"@{_name}/conf", "tags": [f"{_name}__conf"]}
        ]).build()

        execute_rampup = _rampup_phase == "yes"
        execute_main = _main_phase == "yes"

        ops_per_client = _ops / _clients

        if pd.isna(_rampup_rate_limit):
            rampup_rate_limit = 0
        elif _rampup_rate_limit.startswith("infer="):
            rampup_rate_limit = Infer(csv_input, filetree.path("root")) \
                .infer_from_expr(_rampup_rate_limit.split("=")[1])
        else:
            rampup_rate_limit = _rampup_rate_limit

        if pd.isna(_main_rate_limit):
            main_rate_limit = 0
        elif _main_rate_limit.startswith("infer="):
            main_rate_limit = Infer(csv_input, filetree.path("root")) \
                .infer_from_expr(_main_rate_limit.split("=")[1])
        else:
            main_rate_limit = _main_rate_limit

        main_rate_limit_per_client = main_rate_limit / _clients

        cassandra_hosts = resources.roles["cassandra"][:_hosts]
        nb_hosts = resources.roles["clients"][:_clients]

        nb_driver_config_file = LOCAL_FILETREE.path("driver-conf") / _driver_config_file
        nb_workload_file = LOCAL_FILETREE.path("workload-conf") / _workload_config_file

        # Save config
        filetree.copy([
            LOCAL_FILETREE.path("cassandra-conf") / _config_file,
            LOCAL_FILETREE.path("cassandra-conf") / "jvm-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "jvm11-server.options",
            LOCAL_FILETREE.path("cassandra-conf") / "metrics-reporter-config.yaml",
            LOCAL_FILETREE.path("driver-conf") / _driver_config_file,
            LOCAL_FILETREE.path("workload-conf") / _workload_config_file
        ], f"{_name}__conf")

        # Save input parameters
        csv_input.filter([_id]).to_csv(filetree.path(_name) / "input.csv")

        for run_index in range(_repeat):
            filetree.define([
                {"path": f"@{_name}/run-{run_index}", "tags": [f"{_name}-{run_index}"]},
                {"path": f"@{_name}-{run_index}/tmp", "tags": [f"{_name}-{run_index}__tmp"]},
                {"path": f"@{_name}-{run_index}__tmp/dstat", "tags": [f"{_name}-{run_index}__dstat"]},
                {"path": f"@{_name}-{run_index}__tmp/data", "tags": [f"{_name}-{run_index}__data"]},
                {"path": f"@{_name}-{run_index}__tmp/metrics", "tags": [f"{_name}-{run_index}__metrics"]},
                {"path": f"@{_name}-{run_index}/clients", "tags": [f"{_name}-{run_index}__clients"]},
                {"path": f"@{_name}-{run_index}/hosts", "tags": [f"{_name}-{run_index}__hosts"]}
            ]).build()

            logging.info(f"Running {_name}#{_id} - run {run_index}.")

            # Deploy NoSQLBench
            nb = NoSQLBench(docker_image="adugois1/nosqlbench:latest")
            nb.deploy(nb_hosts)

            nb_driver = nb.filetree("remote_container").path("driver-conf") / nb_driver_config_file.name
            nb_workload = nb.filetree("remote_container").path("workload-conf") / nb_workload_file.name
            nb_data = nb.filetree("remote_container").path("data")

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
                schema_options = dict(driver="cqld4",
                                      workload=nb_workload,
                                      alias="xp2",
                                      tags="block:schema",
                                      driverconfig=nb_driver,
                                      threads=1,
                                      rf=_rf,
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
                                      cycles=_keys,
                                      cyclerate=rampup_rate_limit,
                                      stride=_client_stride,
                                      keysize=_key_size_in_bytes,
                                      valuesize=_value_size_in_bytes,
                                      errors="warn,retry",
                                      host=cassandra.get_host_address(0),
                                      localdc="datacenter1")

                rampup_cmd = RunCommand.from_options(**rampup_options)

                nb.single_command(name="nb-rampup",
                                  command=rampup_cmd,
                                  driver_path=nb_driver_config_file,
                                  workload_path=nb_workload_file)

                # Flush memtable to SSTable
                cassandra.nodetool("flush -- baselines keyvalue")

                # Perform a major compaction
                cassandra.nodetool("compact")

                # Ensure compaction is done
                time.sleep(30)

            logging.info(cassandra.tablestats("baselines.keyvalue"))
            logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

            if execute_main:
                main_options = dict(driver="cqld4",
                                    workload=nb_workload,
                                    alias="xp2",
                                    tags="block:main-read",
                                    driverconfig=nb_driver,
                                    threads=_client_threads,
                                    stride=_client_stride,
                                    keydist=_key_dist,
                                    keysize=_key_size_in_bytes,
                                    valuesize=_value_size_in_bytes,
                                    readratio=_read_ratio,
                                    writeratio=_write_ratio,
                                    errors="timer",
                                    host=cassandra.get_host_address(0),
                                    localdc="datacenter1")

                if main_rate_limit_per_client > 0:
                    main_options["cyclerate"] = main_rate_limit_per_client

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

                _tmp_dstat_path = filetree.path(f"{_name}-{run_index}__dstat")
                _tmp_data_path = filetree.path(f"{_name}-{run_index}__data")
                _tmp_metrics_path = filetree.path(f"{_name}-{run_index}__metrics")
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
                _client_path = filetree.path(f"{_name}-{run_index}__clients")
                _host_path = filetree.path(f"{_name}-{run_index}__hosts")

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

                filetree.remove(f"{_name}-{run_index}__tmp")

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

    DEFAULT_JOB_NAME = "cassandra"
    DEFAULT_SITE = "nancy"
    DEFAULT_CLUSTER = "gros"
    DEFAULT_ENV_NAME = "debian11-x64-min"
    DEFAULT_WALLTIME = "00:30:00"
    DEFAULT_REPORT_INTERVAL = 30
    DEFAULT_HISTOGRAM_FILTER = ".*result:30s"

    set_config(ansible_stdout="noop")

    parser = argparse.ArgumentParser()

    parser.add_argument("input", type=str)
    parser.add_argument("--job-name", type=str, default=DEFAULT_JOB_NAME)
    parser.add_argument("--site", type=str, default=DEFAULT_SITE)
    parser.add_argument("--cluster", type=str, default=DEFAULT_CLUSTER)
    parser.add_argument("--env-name", type=str, default=DEFAULT_ENV_NAME)
    parser.add_argument("--reservation", type=str, default=None)
    parser.add_argument("--walltime", type=str, default=DEFAULT_WALLTIME)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--report-interval", type=int, default=DEFAULT_REPORT_INTERVAL)
    parser.add_argument("--histogram-filter", type=str, default=DEFAULT_HISTOGRAM_FILTER)
    parser.add_argument("--id", type=str, action="append", default=None)
    parser.add_argument("--from-id", type=str, default=None)
    parser.add_argument("--to-id", type=str, default=None)
    parser.add_argument("--log", type=str, default=None)

    args = parser.parse_args()

    csv_input = CSVInput(Path(args.input))
    csv_input.create_view("filtered_sets", csv_input.get_ids(from_id=args.from_id, to_id=args.to_id, ids=args.id))

    settings = dict(job_name=args.job_name, env_name=args.env_name, walltime=args.walltime)

    if args.reservation is not None:
        settings["reservation"] = args.reservation

    if args.output is None:
        now = datetime.now().isoformat(timespec='seconds')
        output_path = LOCAL_FILETREE.path("output") / f"{csv_input.file_path.stem}.{now}"
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
