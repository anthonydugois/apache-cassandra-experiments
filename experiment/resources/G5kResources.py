import logging
from pathlib import Path
from typing import Optional

import enoslib as en
from yaml import safe_load


class NoRoleException(Exception):
    pass


class NodeCountException(Exception):
    pass


class UndefinedProviderException(Exception):
    pass


class G5kResources:
    DEFAULT_BIND_VAR_DOCKER = "/tmp/docker"
    DEFAULT_DOCKER_REGISTRY = dict(type="external", ip="docker-cache.grid5000.fr", port=80)

    def __init__(self, site: str, cluster: str, settings: dict,
                 bind_var_docker: Optional[str] = None,
                 docker_registry: Optional[dict] = None):
        if bind_var_docker is None:
            bind_var_docker = G5kResources.DEFAULT_BIND_VAR_DOCKER
        if docker_registry is None:
            docker_registry = G5kResources.DEFAULT_DOCKER_REGISTRY

        self.site = site
        self.cluster = cluster

        self.net_conf = en.G5kNetworkConf(type="prod", roles=["main-net"], site=self.site)
        self.conf = en.G5kConf.from_settings(**settings).add_network_conf(self.net_conf)

        self.bind_var_docker = bind_var_docker
        self.docker_registry = docker_registry

        self.provider: Optional[en.G5k] = None
        self.roles: Optional[en.Roles] = None
        self.networks: Optional[en.Networks] = None
        self.role_counts: dict[str, int] = {}

    def add_fixed_machines(self, roles: list[str], node_count: int, start_index: int = 1,
                           cluster: Optional[str] = None):
        if len(roles) <= 0:
            raise NoRoleException
        if node_count <= 0:
            raise NodeCountException
        if cluster is None:
            cluster = self.cluster

        servers = [f"{cluster}-{start_index + index}.{self.site}.grid5000.fr"
                   for index in range(node_count)]

        logging.info(f"Adding {servers} to {roles}.")

        self.conf = self.conf.add_machine(roles=roles, servers=servers, primary_network=self.net_conf)

        self._update_role_count(roles, node_count)

    def add_machines(self, roles: list[str], node_count: int, cluster: Optional[str] = None):
        if len(roles) <= 0:
            raise NoRoleException
        if node_count <= 0:
            raise NodeCountException
        if cluster is None:
            cluster = self.cluster

        logging.info(f"Adding {node_count} machines from cluster {cluster} to {roles}.")

        self.conf = self.conf.add_machine(roles=roles, cluster=cluster, nodes=node_count, primary_network=self.net_conf)

        self._update_role_count(roles, node_count)

    def _update_role_count(self, roles: list[str], node_count: int):
        for role in roles:
            if role in self.role_counts:
                self.role_counts[role] += node_count
            else:
                self.role_counts[role] = node_count

    def count(self, role: str):
        if role in self.role_counts:
            return self.role_counts[role]

        return 0

    def acquire(self, with_docker: Optional[str] = None):
        self.provider = en.G5k(self.conf.finalize())
        self.roles, self.networks = self.provider.init()

        if with_docker is not None:
            logging.info("Installing Docker...")

            with Path(".credentials").open("r") as file:
                credentials = safe_load(file)

            docker = en.Docker(agent=self.roles[with_docker],
                               bind_var_docker=self.bind_var_docker,
                               registry_opts=self.docker_registry,
                               credentials=credentials)

            docker.deploy()

            for role in self.roles[with_docker]:
                logging.info(f"[{role.address}] Docker host is ready.")

        return self.roles, self.networks

    def release(self):
        if self.provider is None:
            raise UndefinedProviderException

        self.provider.destroy()

        self.provider = None
        self.roles = None
        self.networks = None
