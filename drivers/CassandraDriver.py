import logging
import shutil
import time
from pathlib import Path
from typing import Optional, Union

import enoslib as en

import drivers.util as util
from drivers import Driver


def host_addresses(hosts: list[en.Host], port=0):
    """
    Transform a list of EnOSlib roles to a list of string addresses.
    """

    _port = f":{port}" if port > 0 else ""
    return [f"{host.address}{_port}" for host in hosts]


class MissingHostsException(Exception):
    pass


class InvalidSeedCountException(Exception):
    pass


class CassandraDriver(Driver):
    CONTAINER_NAME = "cassandra"
    START_DELAY_IN_SECONDS = 120

    def __init__(self, docker_image: str):
        super().__init__()

        self.docker_image = docker_image
        self.local_global_root_path = Path(f"tmp_{id(self)}")

        self.seeds: Optional[list[en.Host]] = None
        self.not_seeds: Optional[list[en.Host]] = None

    @property
    def seed_count(self):
        return len(self.seeds) if self.seeds is not None else 0

    @property
    def not_seed_count(self):
        return len(self.not_seeds) if self.not_seeds is not None else 0

    def set_seeds(self, seeds: list[en.Host]):
        self.seeds = seeds

    def set_not_seeds(self, not_seeds: list[en.Host]):
        self.not_seeds = not_seeds

    def get_host_address(self, index: int):
        return self.hosts[index].address

    def build_file_tree(self, conf_dir="conf"):
        for host in self.hosts:
            local_root_path = self.local_global_root_path / host.address

            local_conf_path = local_root_path / conf_dir
            local_conf_path.mkdir(parents=True, exist_ok=True)

            host.extra.update(local_root_path=str(local_root_path))
            host.extra.update(local_conf_path=str(local_conf_path))

            remote_root_path = "/root/cassandra"
            remote_conf_path = f"{remote_root_path}/{conf_dir}"

            remote_data_path = "/tmp/storage-data"  # Warning: make sure there is enough space on disk
            remote_static_path = "/tmp/static-data"
            remote_metrics_path = "/tmp/metrics-data/metrics"

            host.extra.update(remote_root_path=remote_root_path)
            host.extra.update(remote_conf_path=remote_conf_path)
            host.extra.update(remote_data_path=remote_data_path)
            host.extra.update(remote_static_path=remote_static_path)
            host.extra.update(remote_metrics_path=remote_metrics_path)

            host.extra.update(remote_container_conf_path="/etc/cassandra")
            host.extra.update(remote_container_data_path="/var/lib/cassandra")
            host.extra.update(remote_container_static_path="/var/lib/static-data")
            host.extra.update(remote_container_metrics_path="/metrics")

    def init(self, hosts: list[en.Host], seed_count=1, reset=False):
        if len(hosts) <= 0:
            raise MissingHostsException

        if seed_count < 1 or seed_count > len(hosts):
            raise InvalidSeedCountException

        seeds, not_seeds = hosts[:seed_count], hosts[seed_count:]

        self.set_hosts(hosts)
        self.set_seeds(seeds)

        if len(not_seeds) > 0:
            self.set_not_seeds(not_seeds)

        self.build_file_tree()

        if reset:
            self.reset_data()

    def reset_data(self):
        with en.actions(roles=self.hosts) as actions:
            # Remove existing data
            actions.file(path="{{remote_data_path}}", state="absent")

    def create_config(self, template_path: Union[str, Path]):
        seed_addresses = ",".join(host_addresses(self.seeds, port=7000))

        for host in self.hosts:
            local_conf_path = host.extra["local_conf_path"]

            util.build_yaml(template_path=template_path,
                            output_path=Path(local_conf_path, "cassandra.yaml"),
                            update_spec={
                                "seed_provider": {0: {"parameters": {0: {"seeds": seed_addresses}}}},
                                "listen_address": host.address,
                                "rpc_address": host.address
                            })

    def create_extra_config(self, template_paths: list[Union[str, Path]]):
        for host in self.hosts:
            local_conf_path = host.extra["local_conf_path"]

            for template_path in template_paths:
                shutil.copy(template_path, local_conf_path)

    def deploy(self):
        """
        Deploy a cluster of Cassandra nodes. Make some system optimizations to run Cassandra properly.

        1. Configure nodes for running Cassandra:
            a. Disable the swap memory.
            b. Increase number of memory map areas.
        2. Create Cassandra containers.
        """

        with en.actions(roles=self.hosts) as actions:
            actions.file(path="{{remote_root_path}}", state="directory")
            actions.file(path="{{remote_data_path}}", state="directory", mode="777")
            actions.file(path="{{remote_static_path}}", state="directory", mode="777")
            actions.file(path="{{remote_metrics_path}}", state="directory", mode="777")

            # Transfer files
            actions.copy(src="{{local_root_path}}/", dest="{{remote_root_path}}")

            # Disable the swap memory
            actions.shell(cmd="swapoff --all")

            # Increase number of memory map areas
            actions.sysctl(name="vm.max_map_count", value="1048575")

            # Create Cassandra container (without running)
            actions.docker_container(name=CassandraDriver.CONTAINER_NAME,
                                     image=self.docker_image,
                                     state="present",
                                     detach="yes",
                                     network_mode="host",
                                     mounts=[
                                         {
                                             "source": "{{remote_conf_path}}/cassandra.yaml",
                                             "target": "{{remote_container_conf_path}}/cassandra.yaml",
                                             "type": "bind"
                                         },
                                         {
                                             "source": "{{remote_conf_path}}/jvm-server.options",
                                             "target": "{{remote_container_conf_path}}/jvm-server.options",
                                             "type": "bind"
                                         },
                                         {
                                             "source": "{{remote_conf_path}}/jvm11-server.options",
                                             "target": "{{remote_container_conf_path}}/jvm11-server.options",
                                             "type": "bind"
                                         },
                                         {
                                             "source": "{{remote_conf_path}}/metrics-reporter-config.yaml",
                                             "target": "{{remote_container_conf_path}}/metrics-reporter-config.yaml",
                                             "type": "bind"
                                         },
                                         {
                                             "source": "{{remote_data_path}}",
                                             "target": "{{remote_container_data_path}}",
                                             "type": "bind"
                                         },
                                         {
                                             "source": "{{remote_static_path}}",
                                             "target": "{{remote_container_static_path}}",
                                             "type": "bind"
                                         },
                                         {
                                             "source": "{{remote_metrics_path}}",
                                             "target": "{{remote_container_metrics_path}}",
                                             "type": "bind"
                                         }
                                     ],
                                     ulimits=[
                                         "memlock:-1:-1",
                                         "nofile:100000:100000",
                                         "nproc:32768:32768",
                                         "as:-1:-1"
                                     ])

        logging.info("Cassandra has been deployed.")

    def start(self):
        """
        Run a Cassandra cluster.

        Note that running Cassandra should be done one node at a time;
        this is due to the bootstrapping process which may generate
        collisions when two nodes are starting at the same time.

        For more details, see:

        - https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/initialize/initSingleDS.html
        - https://thelastpickle.com/blog/2017/05/23/auto-bootstrapping-part1.html
        """

        for index, host in enumerate(self.hosts):
            with en.actions(roles=host) as actions:
                actions.docker_container(name=CassandraDriver.CONTAINER_NAME, state="started")

            # Make sure to wait at least 2 minutes for bootstrapping to finish
            time.sleep(CassandraDriver.START_DELAY_IN_SECONDS)

            logging.info(f"[{host.address}] Cassandra is up and running "
                         f"({index + 1}/{self.host_count}).")

    def cleanup(self):
        shutil.rmtree(self.local_global_root_path)

    def destroy(self):
        """
        Destroy a Cassandra instance.

        1. Stop and remove the Cassandra Docker container.
        2. Remove Cassandra configuration files, caches and metrics.
        3. Drop OS caches.

        Note that this does not remove data.
        """

        with en.actions(roles=self.hosts) as actions:
            # Stop and remove container
            actions.docker_container(name=CassandraDriver.CONTAINER_NAME, state="absent")

            # Remove Cassandra files
            actions.file(path="{{remote_root_path}}", state="absent")

            # Remove Cassandra caches
            actions.file(path="{{remote_data_path}}/saved_caches", state="absent")
            actions.file(path="{{remote_data_path}}/hints", state="absent")

            # Remove Cassandra metrics
            actions.file(path="{{remote_metrics_path}}", state="absent")

            # Wait some time to be sure that files are not needed anymore by page cache
            time.sleep(10)

            # Drop OS caches
            actions.shell(cmd="echo 3 > /proc/sys/vm/drop_caches")

    def nodetool(self, command: str, hosts: Optional[list[en.Host]] = None):
        """
        Execute the nodetool utility on Cassandra nodes.
        """

        if hosts is None:
            hosts = self.hosts

        with en.actions(roles=hosts) as actions:
            actions.shell(cmd=f"docker exec {CassandraDriver.CONTAINER_NAME} nodetool {command}")
            results = actions.results

        return results

    def flush(self, keyspace: str, table: str):
        """
        Flush memtable to disk on each host.
        """

        self.nodetool(f"flush -- {keyspace} {table}")

    def status(self):
        results = self.nodetool("status", [self.hosts[0]])
        return results[0].payload["stdout"]

    def tablestats(self, keyspace: str, table: str):
        results = self.nodetool(f"tablestats {keyspace}.{table}", [self.hosts[0]])
        return results[0].payload["stdout"]

    def logs(self):
        with en.actions(roles=self.hosts[0]) as actions:
            actions.shell(cmd=f"docker logs {CassandraDriver.CONTAINER_NAME}")
            results = actions.results

        return results[0].payload["stdout"]

    def du(self, path: str):
        with en.actions(roles=self.hosts[0]) as actions:
            actions.shell(cmd=f"docker exec {CassandraDriver.CONTAINER_NAME} du -sh {path}")
            results = actions.results

        return results[0].payload["stdout"]

    def pull_results(self, basepath: Path):
        for host in self.hosts:
            local_path = basepath / host.address
            local_path.mkdir(parents=True, exist_ok=True)

            host.extra.update(local_path=str(local_path))

        self.pull(dest="{{local_path}}", src="{{remote_metrics_path}}")
