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


def run(site: str,
        cluster: str,
        settings: dict,
        parameters: pd.DataFrame,
        rows: pd.DataFrame,
        output_path: str,
        report_interval=30,
        histogram_filter=".*result:30s"):
    _output_path = pathlib.Path(output_path)

    # Warning: the two following values must be wrapped in an int, as pandas returns an np.int64, which is not usable in
    # the resource driver.
    max_hosts = int(rows["hosts"].max())
    max_clients = int(rows["clients"].max())

    # Acquire G5k resources
    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["nodes", "cassandra"], max_hosts)
    resources.add_machines(["nodes", "clients"], max_clients)

    resources.acquire(with_docker="nodes")

    # Run experiments
    for _id, row in rows.iterrows():
        _name = row["name"]
        _hosts = row["hosts"]
        _clients = row["clients"]
        _ops = row["ops"]
        _throughput = row["throughput"]
        _throughput_ref = row["throughput_ref"]
        _rf = row["rf"]
        _value_size_in_bytes = row["value_size_in_bytes"]
        _bytes_per_host = row["bytes_per_host"]
        _docker_image = row["docker_image"]
        _config_file = row["config_file"]
        _driver = row["driver"]
        _workload = row["workload"]

        logging.info(f"Running {_name} (#{_id})...")

        rf = min(_rf, _hosts)
        bytes_total = _hosts * _bytes_per_host / rf
        key_count = round(bytes_total / _value_size_in_bytes)
        result_path = _output_path / _name

        # Infer saturating throughput
        sat_throughput = 0
        if _throughput_ref != _id:
            ref_match = parameters[parameters.index == _throughput_ref]

            if ref_match.empty:
                logging.error(f"Reference #{_throughput_ref} does not exist.")
            else:
                ref_row = ref_match.iloc[0]
                ref_path = pathlib.Path(output_path, ref_row["name"])

                if ref_path.exists():
                    # Retrieve saturating throughput
                    ref_df = pd.read_csv(ref_path / "data" / "csv" / f"{ROOT.name}.result.csv", index_col=False)
                    sat_throughput = ref_df[ref_df["count"] >= ref_row["ops"]].iloc[0]["mean_rate"]

                    logging.info(f"Saturating throughput currently set to {sat_throughput}.")
                else:
                    logging.warning(f"Reference #{_throughput_ref} has not been executed yet."
                                    "Could not infer saturating throughput.")

        # Deploy and start Cassandra
        cassandra = Cassandra(name="cassandra", docker_image=_docker_image)

        cassandra.init(resources.roles["cassandra"][:_hosts])
        cassandra.create_config(ROOT / _config_file)
        cassandra.deploy_and_start()

        logging.info(cassandra.nodetool("status"))

        # Deploy NoSQLBench
        nb = NoSQLBench(name="nb",
                        docker_image="nosqlbench/nosqlbench:nb5preview",
                        driver_path=ROOT / "config" / "driver",
                        workload_path=ROOT / "workloads")

        nb.init(resources.roles["clients"][:_clients])
        nb.deploy()

        # Create schema
        schema_options = dict(driver="cqld4",
                              workload=nb.workload(_workload),
                              alias=ROOT.name,
                              tags="block:schema",
                              driverconfig=nb.driver(_driver),
                              threads=1,
                              rf=rf,
                              host=cassandra.get_host_address(0),
                              localdc="datacenter1")

        schema_cmd = RunCommand.from_options(**schema_options)

        nb.command(schema_cmd)

        # Insert data
        rampup_options = dict(driver="cqld4",
                              workload=nb.workload(_workload),
                              alias=ROOT.name,
                              tags="block:rampup",
                              driverconfig=nb.driver(_driver),
                              threads="auto",
                              cycles=key_count,
                              cyclerate=50000,
                              keycount=key_count,
                              valuesize=_value_size_in_bytes,
                              host=cassandra.get_host_address(0),
                              localdc="datacenter1")

        rampup_cmd = RunCommand.from_options(**rampup_options)

        nb.command(rampup_cmd)

        logging.info(cassandra.nodetool("tablestats baselines.keyvalue"))
        logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

        # Main experiment
        main_options = dict(driver="cqld4",
                            workload=nb.workload(_workload),
                            alias=ROOT.name,
                            tags="block:main-read",
                            driverconfig=nb.driver(_driver),
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

        with en.Dstat(nodes=nb.hosts, options="-aT", backup_dir=result_path / "dstat"):
            time.sleep(5)
            nb.command(main_cmd)
            time.sleep(5)

        # Get results
        nb.sync_results(result_path)

        # Save logs
        with (result_path / "cassandra.log").open("w") as file:
            file.write(cassandra.logs())

        # Save input parameters
        parameters[parameters.index == _id].to_csv(result_path / "input.csv")

        # Destroy instances
        logging.info("Destroying instances...")

        nb.destroy()
        cassandra.destroy()

    # Release resources
    resources.release()

    # Compress results
    with tarfile.open(_output_path.parent / "raw.tar.gz", mode="w:gz") as file:
        file.add(_output_path, arcname=_output_path.name)


if __name__ == "__main__":
    import argparse

    from sys import stdout
    from enoslib.config import set_config

    set_config(ansible_stdout="noop")
    logging.basicConfig(stream=stdout, level=logging.INFO, format="%(asctime)s %(levelname)s : %(message)s")

    parser = argparse.ArgumentParser()

    parser.add_argument("input", type=str)
    parser.add_argument("--job-name", type=str, default="cassandra")
    parser.add_argument("--site", type=str, default="nancy")
    parser.add_argument("--cluster", type=str, default="gros")
    parser.add_argument("--env-name", type=str, default="debian11-x64-min")
    parser.add_argument("--reservation", type=str, default=None)
    parser.add_argument("--walltime", type=str, default="00:30:00")
    parser.add_argument("--output", type=str, default=str(ROOT / "output" / "raw"))
    parser.add_argument("--report-interval", type=int, default=30)
    parser.add_argument("--histogram-filter", type=str, default=".*result:30s")
    parser.add_argument("--id", type=str, action="append", default=None)
    parser.add_argument("--from-id", type=str, default=None)
    parser.add_argument("--to-id", type=str, default=None)

    args = parser.parse_args()

    parameters = pd.read_csv(args.input, index_col=0, dtype={"id": str, "throughput_ref": str})

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
        rows=rows,
        output_path=args.output,
        report_interval=args.report_interval,
        histogram_filter=args.histogram_filter)
