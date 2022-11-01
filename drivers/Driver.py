from typing import Optional, Union

import enoslib as en

from util.filetree import FileTree


class MissingHostsException(Exception):
    pass


class MissingFileTreeException(Exception):
    pass


class Driver:
    def __init__(self):
        self.filetrees: dict[str, FileTree] = {}
        self.mount_points: dict[str, dict] = {}
        self.hosts: Optional[list[en.Host]] = None

    @property
    def host_count(self):
        return len(self.hosts) if self.hosts is not None else 0

    def set_hosts(self, hosts: list[en.Host]):
        if len(hosts) <= 0:
            raise MissingHostsException

        self.hosts = hosts

    def create_filetree(self, key: str, spec: list[dict]):
        filetree = FileTree().define(spec)
        self.filetrees[key] = filetree

        return filetree

    def filetree(self, key: str):
        if key in self.filetrees:
            return self.filetrees[key]

        raise MissingFileTreeException

    def create_mount_points(self, mount_points):
        for mount_point in mount_points:
            self.create_mount_point(*mount_point)

    def create_mount_point(self, key: str, source: str, target: str, type="bind"):
        mount_point = dict(source=source, target=target, type=type)
        self.mount_points[key] = mount_point

        return mount_point

    def mounts(self):
        return list(self.mount_points.values())

    def push(self, src: str,
             dest: str,
             src_hosts: Optional[list[Union[None, en.Host]]] = None,
             dest_hosts: Optional[list[en.Host]] = None):
        """
        Push files from `src` on `src_hosts` to `dest` on `dest_hosts`.
        """

        if src_hosts is None:
            src_hosts = [None]
        if dest_hosts is None:
            dest_hosts = self.hosts

        for src_host in src_hosts:
            args = dict(src=src, dest=dest, mode="push")
            if isinstance(src_host, en.Host):
                args["delegate_to"] = src_host.address

            with en.actions(roles=dest_hosts) as actions:
                actions.synchronize(**args)

    def pull(self, dest: str,
             src: str,
             dest_hosts: Optional[list[Union[None, en.Host]]] = None,
             src_hosts: Optional[list[en.Host]] = None):
        """
        Pull files from `src` on `src_hosts` to `dest` on `dest_hosts`.
        """

        if dest_hosts is None:
            dest_hosts = [None]
        if src_hosts is None:
            src_hosts = self.hosts

        for dest_host in dest_hosts:
            args = dict(src=src, dest=dest, mode="pull")
            if isinstance(dest_host, en.Host):
                args["delegate_to"] = dest_host.address

            with en.actions(roles=src_hosts) as actions:
                actions.synchronize(**args)
