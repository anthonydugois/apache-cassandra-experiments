import logging
import pathlib
import time
import sys
import argparse
import enoslib as en
import enoslib.config as en_conf

from drivers.Resources import Resources
from drivers.Cassandra import Cassandra
from drivers.NoSQLBench import NoSQLBench, RunCommand

en_conf.set_config(g5k_cache=False, ansible_stdout="noop")

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


def run_xp(site: str, cluster: str, settings: dict, host_count: int, client_count: int):
    resources = Resources(site=site, cluster=cluster, settings=settings)

    resources.add_machines(["hosts", "cassandra"], host_count)
    resources.add_machines(["hosts", "clients"], client_count)

    resources.get(with_docker="hosts")
    
    for kind in ["se", "base"]:
        logging.info(f"Running {kind} scenario...")

        hosts = resources.roles["cassandra"]
        seeds = resources.roles["cassandra"][0:1]
        not_seeds = resources.roles["cassandra"][1:host_count]
        clients = resources.roles["clients"]

        cassandra = Cassandra(name="cassandra", docker_image=f"adugois1/apache-cassandra-{kind}:latest")

        cassandra.set_hosts(hosts)
        cassandra.set_seeds(seeds)

        if len(not_seeds) > 0:
            cassandra.set_not_seeds(not_seeds)

        cassandra.build_file_tree()
        cassandra.create_config(f"templates/1_baseline_saturation/cassandra/conf/cassandra-{kind}.yaml")

        cassandra.deploy_and_start()

        logging.info(cassandra.nodetool("status"))

        nb = NoSQLBench(name="nb", docker_image="nosqlbench/nosqlbench:nb5preview")

        nb.set_hosts(clients)
        nb.deploy()
        
        host_address = cassandra.hosts[0].address
        keycount = 35000000
        opcount = 10000000

        nb.command(RunCommand.from_options(driver="cqld4",
                                           workload=f"{nb.remote_container_conf_path}/workloads/baseline",
                                           tags="block:schema",
                                           threads=1,
                                           rf=3,
                                           host=host_address,
                                           localdc="datacenter1"))

        nb.command(RunCommand.from_options(driver="cqld4",
                                           workload=f"{nb.remote_container_conf_path}/workloads/baseline",
                                           tags="block:rampup",
                                           threads="auto",
                                           cycles=keycount,
                                           cyclerate=50000,
                                           keycount=keycount,
                                           host=host_address,
                                           localdc="datacenter1"))

        logging.info(cassandra.nodetool("tablestats baselines.keyvalue"))
        logging.info(cassandra.du("/var/lib/cassandra/data/baselines"))

        with en.Dstat(nodes=nb.hosts, options="-aT", backup_dir=pathlib.Path("results", "1_baseline_saturation", kind, "dstat")) as dstat:
            time.sleep(5)

            nb.command(RunCommand.from_options(driver="cqld4",
                                               workload=f"{nb.remote_container_conf_path}/workloads/baseline",
                                               tags="block:main-read",
                                               threads="auto",
                                               cycles=opcount,
                                               keycount=keycount,
                                               host=host_address,
                                               localdc="datacenter1")
                       .logs_dir(nb.remote_container_data_path)
                       .log_histograms(f"{nb.remote_container_data_path}/histograms.csv:.*result:30s")
                       .log_histostats(f"{nb.remote_container_data_path}/histostats.csv:.*result:30s")
                       .report_summary_to(f"{nb.remote_container_data_path}/summary.txt")
                       .report_csv_to(f"{nb.remote_container_data_path}/csv")
                       .report_interval(30))

            time.sleep(5)

        nb.sync_results(f"./results/1_baseline_saturation/{kind}")

        nb.destroy()

        cassandra.destroy()

    resources.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--job-name", type=str, default="cassandra")
    parser.add_argument("--site", type=str, default="nancy")
    parser.add_argument("--cluster", type=str, default="gros")
    parser.add_argument("--env-name", type=str, default="debian11-x64-min")
    parser.add_argument("--reservation", type=str, default="now")
    parser.add_argument("--walltime", type=str, default="00:30:00")
    parser.add_argument("--hosts", type=int)
    parser.add_argument("--clients", type=int)

    args = parser.parse_args()

    run_xp(site=args.site,
           cluster=args.cluster,
           settings=dict(job_name=args.job_name,
                         env_name=args.env_name,
                         reservation=args.reservation,
                         walltime=args.walltime),
           host_count=args.hosts,
           client_count=args.clients)
