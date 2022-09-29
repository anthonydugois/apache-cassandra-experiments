from typing import Optional

import logging
import enoslib as en

DEFAULT_DOCKER_REGISTRY = dict(type="external", ip="docker-cache.grid5000.fr", port=80)


class UndefinedProviderException(Exception):
    pass


class Resources:
    def __init__(self, site: str, cluster: str, settings: dict, docker_registry: Optional[dict] = None):
        if docker_registry is None:
            docker_registry = DEFAULT_DOCKER_REGISTRY

        self.site = site
        self.cluster = cluster

        self.network_conf = en.G5kNetworkConf(type="prod", roles=["main-net"], site=self.site)
        self.conf = en.G5kConf.from_settings(**settings).add_network_conf(self.network_conf)

        self.docker_registry = docker_registry

        self.provider: Optional[en.G5k] = None
        self.roles: Optional[en.Roles] = None
        self.networks: Optional[en.Networks] = None
        self.role_counts: dict[str, int] = {}

    def add_machines(self, roles: list[str], node_count: int, cluster: Optional[str] = None):
        if cluster is None:
            cluster = self.cluster

        self.conf = self.conf.add_machine(roles=roles, cluster=cluster, nodes=node_count,
                                          primary_network=self.network_conf)

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

            docker = en.Docker(agent=self.roles[with_docker], bind_var_docker="/tmp/docker",
                               registry_opts=self.docker_registry)

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
