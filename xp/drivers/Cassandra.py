from typing import Optional

import logging
import pathlib
import shutil
import time
import util
import enoslib as en


def host_addresses(hosts: list[en.Host], port=0):
    """
    Transform a list of EnOSlib roles to a list of string addresses.
    """

    _port = f":{port}" if port > 0 else ""

    return [f"{host.address}{_port}" for host in hosts]


class Cassandra:
    def __init__(self, name: str, docker_image: str, local_global_root_path="tmp/cassandra"):
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

    def set_hosts(self, hosts: list[en.Host]):
        self.hosts = hosts

    def set_seeds(self, seeds: list[en.Host]):
        self.seeds = seeds

    def set_not_seeds(self, not_seeds: list[en.Host]):
        self.not_seeds = not_seeds

    def build_file_tree(self, conf_dir="conf"):
        for host in self.hosts:
            local_root_path = f"{self.local_global_root_path}/{host.address}"
            local_conf_path = f"{local_root_path}/{conf_dir}"

            remote_root_path = "/root/cassandra"
            remote_conf_path = f"{remote_root_path}/{conf_dir}"

            remote_container_conf_path = "/etc/cassandra"

            host.extra.update(local_root_path=local_root_path)
            host.extra.update(local_conf_path=local_conf_path)

            host.extra.update(remote_root_path=remote_root_path)
            host.extra.update(remote_conf_path=remote_conf_path)

            host.extra.update(remote_container_conf_path=remote_container_conf_path)

            pathlib.Path(local_conf_path).mkdir(parents=True, exist_ok=True)

    def create_config(self, template_path: str):
        seed_addresses = ",".join(host_addresses(self.seeds, port=7000))

        for host in self.hosts:
            local_conf_path = host.extra["local_conf_path"]

            util.build_yaml(template_path=template_path,
                            output_path=f"{local_conf_path}/cassandra.yaml",
                            update_spec={
                                "seed_provider": {0: {"parameters": {0: {"seeds": seed_addresses}}}},
                                "listen_address": host.address,
                                "rpc_address": host.address
                            })

    def deploy(self):
        """
        Deploy a cluster of Cassandra nodes. Make some system
        optimizations to run Cassandra properly.

        1. Configure nodes for running Cassandra:
            a. Disable the swap memory.
            b. Increase number of memory map areas.
        2. Create Cassandra containers.
        """

        with en.actions(roles=self.hosts) as actions:
            actions.file(path="{{remote_root_path}}", state="directory")

            # Transfer configuration files
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
                                         }
                                     ])

        logging.info("Cassandra has been deployed. Ready to start.")

    def start_host(self, host, spawn_time=120):
        """
        Run a Cassandra node. Make sure to wait at least 2 minutes
        in order to let Cassandra start properly.
        """

        logging.info(f"Starting Cassandra on {host.address}...")

        with en.actions(roles=host) as actions:
            actions.docker_container(name=self.name, state="started")

        time.sleep(spawn_time)

        logging.info(f"Cassandra is up and running on host {host.address}.")

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
        pass

    def nodetool(self, command="status"):
        with en.actions(roles=self.hosts[0]) as actions:
            actions.shell(cmd=f"docker exec {self.name} nodetool {command}")

            results = actions.results

        return results[0].payload["stdout"]

    def du(self, path="/var/lib/cassandra/data"):
        with en.actions(roles=self.hosts[0]) as actions:
            actions.shell(cmd=f"docker exec {self.name} du -sh {path}")

            results = actions.results

        return results[0].payload["stdout"]
