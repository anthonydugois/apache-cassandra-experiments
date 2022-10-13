import logging
import pathlib
import shutil
import time
from typing import Optional, Union

import enoslib as en

import drivers.util as util


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


class Cassandra:
    def __init__(self, name: str, docker_image: str, local_global_root_path=pathlib.Path("_tmp")):
        self.name = name
        self.docker_image = docker_image
        self.local_global_root_path = local_global_root_path

        self.hosts: Optional[list[en.Host]] = None
        self.seeds: Optional[list[en.Host]] = None
        self.not_seeds: Optional[list[en.Host]] = None

    @property
    def host_count(self):
        return len(self.hosts) if self.hosts is not None else 0

    @property
    def seed_count(self):
        return len(self.seeds) if self.seeds is not None else 0

    @property
    def not_seed_count(self):
        return len(self.not_seeds) if self.not_seeds is not None else 0

    def get_host_address(self, index: int):
        return self.hosts[index].address

    def set_hosts(self, hosts: list[en.Host]):
        self.hosts = hosts

    def set_seeds(self, seeds: list[en.Host]):
        self.seeds = seeds

    def set_not_seeds(self, not_seeds: list[en.Host]):
        self.not_seeds = not_seeds

    def build_file_tree(self, conf_dir="conf"):
        for host in self.hosts:
            local_root_path = self.local_global_root_path / host.address

            local_conf_path = local_root_path / conf_dir
            local_conf_path.mkdir(parents=True, exist_ok=True)

            host.extra.update(local_root_path=str(local_root_path))
            host.extra.update(local_conf_path=str(local_conf_path))

            remote_root_path = "/root/cassandra"
            remote_conf_path = f"{remote_root_path}/{conf_dir}"
            remote_data_path = f"/tmp/storage-data"  # Warning: make sure there is enough space on disk

            host.extra.update(remote_root_path=remote_root_path)
            host.extra.update(remote_conf_path=remote_conf_path)
            host.extra.update(remote_data_path=remote_data_path)

            host.extra.update(remote_container_conf_path="/etc/cassandra")
            host.extra.update(remote_container_data_path="/var/lib/cassandra")

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

    def create_config(self, template_path: Union[str, pathlib.Path]):
        seed_addresses = ",".join(host_addresses(self.seeds, port=7000))

        for host in self.hosts:
            local_conf_path = host.extra["local_conf_path"]

            util.build_yaml(template_path=template_path,
                            output_path=pathlib.Path(local_conf_path, "cassandra.yaml"),
                            update_spec={
                                "seed_provider": {0: {"parameters": {0: {"seeds": seed_addresses}}}},
                                "listen_address": host.address,
                                "rpc_address": host.address
                            })

    def create_extra_config(self, template_paths: list[Union[str, pathlib.Path]]):
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
            actions.file(path="{{remote_data_path}}", state="directory")

            # Transfer files
            actions.copy(src="{{local_root_path}}/", dest="{{remote_root_path}}")

            # Disable the swap memory
            actions.shell(cmd="swapoff --all")

            # Increase number of memory map areas
            actions.sysctl(name="vm.max_map_count", value="1048575")

            # Create Cassandra container (without running)
            actions.docker_container(name=self.name,
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
                                             "source": "{{remote_data_path}}",
                                             "target": "{{remote_container_data_path}}",
                                             "type": "bind"
                                         }
                                     ],
                                     ulimits=[
                                         "memlock:-1:-1",
                                         "nofile:100000:100000",
                                         "nproc:32768:32768",
                                         "as:-1:-1"
                                     ])

        logging.info("Cassandra has been deployed. Ready to start.")

    def start_host(self, host: en.Host, spawn_time=120):
        """
        Run a Cassandra node. Make sure to wait at least 2 minutes in order to let Cassandra start properly.
        """

        logging.info(f"[{host.address}] Starting Cassandra...")

        with en.actions(roles=host) as actions:
            actions.docker_container(name=self.name, state="started")

        time.sleep(spawn_time)

        logging.info(f"[{host.address}] Cassandra is up and running.")

    def start(self):
        """
        Run a Cassandra cluster. This is done in 2 steps:

        1. Run Cassandra on seed nodes.
        2. Run Cassandra on remaining nodes.

        Note that running Cassandra should be done one node at a time;
        this is due to the bootstrapping process which may generate
        collisions when two nodes are starting at the same time.

        For more details, see:
        - https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/initialize/initSingleDS.html
        - https://thelastpickle.com/blog/2017/05/23/auto-bootstrapping-part1.html
        """

        for host in self.seeds:
            self.start_host(host)

        if self.not_seeds is not None:
            for host in self.not_seeds:
                self.start_host(host)

        logging.info("Cassandra is running!")

    def cleanup(self):
        shutil.rmtree(self.local_global_root_path)

    def deploy_and_start(self, cleanup=True):
        """
        Util to deploy and start Cassandra in one call.
        """

        self.deploy()
        self.start()

        if cleanup:
            self.cleanup()

    def destroy(self):
        """
        Destroy a Cassandra instance. Note that this does not remove data.

        1. Stop and remove the Cassandra Docker container.
        2. Remove the Cassandra configuration files.
        3. Remove the Cassandra caches.
        4. Drop OS caches.
        """

        with en.actions(roles=self.hosts) as actions:
            # Stop and remove container
            actions.docker_container(name=self.name, state="absent")

            # Remove Cassandra files
            actions.file(path="{{remote_root_path}}", state="absent")

            # Remove Cassandra caches
            actions.file(path="{{remote_data_path}}/saved_caches", state="absent")
            actions.file(path="{{remote_data_path}}/hints", state="absent")

            # Wait some time to be sure that files are not needed anymore by page cache
            time.sleep(10)

            # Drop OS caches
            actions.shell(cmd="echo 3 > /proc/sys/vm/drop_caches")

    def nodetool(self, command: str, hosts: Optional[list[en.Host]] = None):
        if hosts is None:
            hosts = self.hosts

        with en.actions(roles=hosts) as actions:
            actions.shell(cmd=f"docker exec {self.name} nodetool {command}")
            results = actions.results

        return results

    def status(self):
        results = self.nodetool("status", [self.hosts[0]])
        return results[0].payload["stdout"]

    def tablestats(self, table: str):
        results = self.nodetool(f"tablestats {table}", [self.hosts[0]])
        return results[0].payload["stdout"]

    def logs(self):
        with en.actions(roles=self.hosts[0]) as actions:
            actions.shell(cmd=f"docker logs {self.name}")
            results = actions.results

        return results[0].payload["stdout"]

    def du(self, path="/var/lib/cassandra/data"):
        with en.actions(roles=self.hosts[0]) as actions:
            actions.shell(cmd=f"docker exec {self.name} du -sh {path}")
            results = actions.results

        return results[0].payload["stdout"]
