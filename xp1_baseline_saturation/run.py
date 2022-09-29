from typing import Optional

import logging
import pathlib
import time
import tarfile
import enoslib as en
import pandas as pd

from drivers.Resources import Resources
from drivers.Cassandra import Cassandra
from drivers.NoSQLBench import NoSQLBench, RunCommand

ROOT = pathlib.Path(__file__).parent

CONFIG = ROOT / "config"
RAW_RESULTS = ROOT / "results" / "raw"

DEFAULT_VERSIONS = {
    "4.2-base": {
        "docker_image": "adugois1/apache-cassandra-base:latest",
        "config_file": CONFIG / "cassandra/cassandra-base.yaml"
    },
    "4.2-se": {
        "docker_image": "adugois1/apache-cassandra-se:latest",
        "config_file": CONFIG / "cassandra/cassandra-se.yaml"
    }
}

DEFAULT_THROUGHPUTS = [1, 0.9, 0.8, 0.7, 0.6]


def run(site: str,
        cluster: str,
        settings: dict,
        host_count: int,
        client_count: int,
        op_count: int,
        value_size_in_bytes=1000,
        bytes_per_host=100e9,
        rf=3,
        report_interval=30,
        histogram_filter=".*result-success:30s",
        versions: Optional[dict] = None,
        throughputs: Optional[list[float]] = None):
    rf = min(rf, host_count)
    bytes_total = host_count * bytes_per_host / rf
    key_count = round(bytes_total / value_size_in_bytes)

    if versions is None:
        versions = DEFAULT_VERSIONS
    if throughputs is None:
        throughputs = DEFAULT_THROUGHPUTS

    # Acquire G5k resources
    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["hosts", "cassandra"], host_count)
    resources.add_machines(["hosts", "clients"], client_count)

    resources.acquire(with_docker="hosts")

    for version in versions:
        logging.info(f"Running scenario on version {version}...")

        saturating_throughput = 0

        for throughput in throughputs:
            result_path = RAW_RESULTS / version / f"{throughput}-throughput"

            # Deploy and start Cassandra
            cassandra = Cassandra(name="cassandra", docker_image=versions[version]["docker_image"])

            cassandra.init(resources.roles["cassandra"])
            cassandra.create_config(versions[version]["config_file"])

            cassandra.deploy_and_start()

            logging.info(cassandra.nodetool("status"))

            # Deploy NoSQLBench
            nb = NoSQLBench(name="nb",
                            docker_image="nosqlbench/nosqlbench:nb5preview",
                            driver_path=CONFIG / "driver",
                            workload_path=ROOT / "workloads")

            nb.init(resources.roles["clients"])

            nb.deploy()

            # Create schema
            schema_options = dict(driver="cqld4",
                                  workload=nb.workload("main"),
                                  alias="main",
                                  tags="block:schema",
                                  driverconfig=nb.driver("main.json"),
                                  threads=1,
                                  rf=rf,
                                  host=cassandra.get_host_address(0),
                                  localdc="datacenter1")

            schema_cmd = RunCommand.from_options(**schema_options)

            nb.command(schema_cmd)

            # Insert data
            rampup_options = dict(driver="cqld4",
                                  workload=nb.workload("main"),
                                  alias="main",
                                  tags="block:rampup",
                                  driverconfig=nb.driver("main.json"),
                                  threads="auto",
                                  cycles=key_count,
                                  cyclerate=50000,
                                  keycount=key_count,
                                  valuesize=value_size_in_bytes,
                                  host=cassandra.get_host_address(0),
                                  localdc="datacenter1")

            rampup_cmd = RunCommand.from_options(**rampup_options)

            nb.command(rampup_cmd)

            logging.info(cassandra.nodetool("tablestats baselines.keyvalue"))
            logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

            # Main experiment
            main_options = dict(driver="cqld4",
                                workload=nb.workload("main"),
                                alias="main",
                                tags="block:main-read",
                                driverconfig=nb.driver("main.json"),
                                threads="auto",
                                cycles=op_count,
                                keycount=key_count,
                                host=cassandra.get_host_address(0),
                                localdc="datacenter1")

            if saturating_throughput > 0:
                main_options["cyclerate"] = throughput * saturating_throughput

            main_cmd = RunCommand \
                .from_options(**main_options) \
                .logs_dir(nb.data()) \
                .log_histograms(nb.data(f"/histograms.csv:{histogram_filter}")) \
                .log_histostats(nb.data(f"/histostats.csv:{histogram_filter}")) \
                .report_summary_to(nb.data("/summary.txt")) \
                .report_csv_to(nb.data("/csv")) \
                .report_interval(report_interval)

            with en.Dstat(nodes=nb.hosts, options="-aT", backup_dir=result_path / "dstat"):
                time.sleep(5)
                nb.command(main_cmd)
                time.sleep(5)

            # Get results
            nb.sync_results(result_path)

            with (result_path / "cassandra.log").open("w") as log_file:
                log_file.write(cassandra.logs())

            if throughput >= 1:
                # Retrieve saturating throughput
                result_df = pd.read_csv(result_path / "data" / "csv" / "main.result.csv", index_col=False)
                saturating_throughput = result_df[result_df["count"] >= key_count].iloc[0]["mean_rate"]

                logging.info(f"Saturating throughput currently set to {saturating_throughput}.")

            # Destroy instances
            logging.info("Destroying instances...")

            nb.destroy()
            cassandra.destroy()

    # Release resources
    resources.release()

    # Compress raw results
    with tarfile.open(ROOT / "results" / "raw.tar.gz", mode="w:gz") as archive_file:
        archive_file.add(RAW_RESULTS, arcname=RAW_RESULTS.name)


if __name__ == "__main__":
    import argparse

    from sys import stdout
    from enoslib.config import set_config

    set_config(ansible_stdout="noop")
    logging.basicConfig(stream=stdout, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser()

    parser.add_argument("--job-name", type=str, default="cassandra")
    parser.add_argument("--site", type=str, default="nancy")
    parser.add_argument("--cluster", type=str, default="gros")
    parser.add_argument("--env-name", type=str, default="debian11-x64-min")
    parser.add_argument("--reservation", type=str, default=None)
    parser.add_argument("--walltime", type=str, default="00:30:00")
    parser.add_argument("--value-size", type=int, default=1000)
    parser.add_argument("--bytes-per-host", type=int, default=100e9)
    parser.add_argument("--rf", type=int, default=3)
    parser.add_argument("--report-interval", type=int, default=30)
    parser.add_argument("--histogram-filter", type=str, default=".*result-success:30s")
    parser.add_argument("--hosts", type=int)
    parser.add_argument("--clients", type=int)
    parser.add_argument("--ops", type=int)

    args = parser.parse_args()

    settings = dict(job_name=args.job_name, env_name=args.env_name, walltime=args.walltime)

    if args.reservation is not None:
        settings["reservation"] = args.reservation

    run(site=args.site,
        cluster=args.cluster,
        settings=settings,
        host_count=args.hosts,
        client_count=args.clients,
        op_count=args.ops,
        value_size_in_bytes=args.value_size,
        bytes_per_host=args.bytes_per_host,
        rf=args.rf,
        report_interval=args.report_interval,
        histogram_filter=args.histogram_filter)
