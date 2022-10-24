from typing import Optional

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

    def create_mount_point(self, key: str, source: str, target: str, type="bind"):
        mount_point = dict(source=source, target=target, type=type)
        self.mount_points[key] = mount_point

        return mount_point

    def mounts(self):
        return list(self.mount_points.values())
