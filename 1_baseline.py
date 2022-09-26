import logging
import pathlib
import time
import enoslib as en

from drivers.Resources import Resources
from drivers.Cassandra import Cassandra
from drivers.NoSQLBench import NoSQLBench, RunCommand


def run_xp(site: str, cluster: str, settings: dict, host_count: int, client_count: int):
    keycount = 100000
    opcount = 10000

    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["hosts", "cassandra"], host_count)
    resources.add_machines(["hosts", "clients"], client_count)

    resources.get(with_docker="hosts")

    hosts = resources.roles["cassandra"]
    seeds = resources.roles["cassandra"][0:1]
    not_seeds = resources.roles["cassandra"][1:host_count]
    clients = resources.roles["clients"]

    cassandra = Cassandra(name="cassandra", docker_image="adugois1/apache-cassandra-base:latest")

    cassandra.set_hosts(hosts)
    cassandra.set_seeds(seeds)

    if len(not_seeds) > 0:
        cassandra.set_not_seeds(not_seeds)

    cassandra.build_file_tree()
    cassandra.create_config("templates/cassandra/conf/cassandra-base.yaml")

    cassandra.deploy_and_start()

    logging.info(cassandra.nodetool("status"))

    nb = NoSQLBench(name="nb", docker_image="nosqlbench/nosqlbench:nb5preview")

    nb.set_hosts(clients)
    nb.deploy()

    nb.command(RunCommand.from_options(driver="cqld4",
                                       workload=f"{nb.remote_container_conf_path}/workloads/baseline",
                                       tags="block:schema",
                                       threads=1,
                                       rf=3,
                                       host=cassandra.hosts[0].address,
                                       localdc="datacenter1"))

    nb.command(RunCommand.from_options(driver="cqld4",
                                       workload=f"{nb.remote_container_conf_path}/workloads/baseline",
                                       tags="block:rampup",
                                       threads="auto",
                                       cycles=keycount,
                                       cyclerate=50000,
                                       keycount=keycount,
                                       host=cassandra.hosts[0].address,
                                       localdc="datacenter1"))

    logging.info(cassandra.nodetool("tablestats baselines.keyvalue"))
    logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

    with en.Dstat(nodes=nb.hosts, options="-aT", backup_dir=pathlib.Path("./results/1_baseline/dstat")) as dstat:
        time.sleep(5)

        nb.command(RunCommand.from_options(driver="cqld4",
                                           workload=f"{nb.remote_container_conf_path}/workloads/baseline",
                                           tags="block:main-read",
                                           threads="auto",
                                           cycles=opcount,
                                           keycount=keycount,
                                           host=cassandra.hosts[0].address,
                                           localdc="datacenter1")
                   .logs_dir(nb.remote_container_data_path)
                   .log_histograms(f"{nb.remote_container_data_path}/histograms.csv")
                   .log_histostats(f"{nb.remote_container_data_path}/histostats.csv")
                   .report_summary_to(f"{nb.remote_container_data_path}/summary.txt")
                   .report_csv_to(f"{nb.remote_container_data_path}/csv")
                   .report_interval(30))

        time.sleep(5)

    nb.sync_results("./results/1_baseline")

    nb.destroy()

    cassandra.destroy()

    # resources.destroy()


if __name__ == "__main__":
    SITE = "nancy"
    CLUSTER = "gros"

    SETTINGS = dict(
        job_name="cassandra",
        job_type="allow_classic_ssh",
        reservation="2022-09-26 15:00:00",
        walltime="1:00:00"
    )

    HOST_COUNT = 3
    CLIENT_COUNT = 1

    run_xp(SITE, CLUSTER, SETTINGS, HOST_COUNT, CLIENT_COUNT)
