from typing import Optional, Union

import logging
import pathlib
import enoslib as en


class Command:
    def __init__(self):
        self.tokens = []

    def option(self, key, value):
        self.tokens.append(f"{key}={value}")

        return self

    def options(self, **kwargs):
        for key in kwargs:
            self.option(key, kwargs[key])

        return self

    def arg(self, name, value):
        self.tokens.append(name)
        self.tokens.append(str(value))

        return self

    def logs_dir(self, value):
        self.arg("--logs-dir", value)

        return self

    def logs_max(self, value):
        self.arg("--logs-max", value)

        return self

    def logs_level(self, value):
        self.arg("--logs-level", value)

        return self

    def report_csv_to(self, value):
        self.arg("--report-csv-to", value)

        return self

    def report_interval(self, value):
        self.arg("--report-interval", value)

        return self

    def log_histograms(self, value):
        self.arg("--log-histograms", value)

        return self

    def log_histostats(self, value):
        self.arg("--log-histostats", value)

        return self

    def report_summary_to(self, value):
        self.arg("--report-summary-to", value)

        return self

    def __str__(self):
        return " ".join(self.tokens)


class RunCommand(Command):
    def __init__(self):
        super().__init__()

        self.tokens.append("run")

    @staticmethod
    def from_options(**kwargs):
        return RunCommand().options(**kwargs)


class MissingHostsException(Exception):
    pass


class NoSQLBench:
    def __init__(self, name: str, docker_image: str, workloads: Union[str, pathlib.Path]):
        self.name = name
        self.docker_image = docker_image
        self.workloads = workloads

        self.remote_root_path = "/root/nosqlbench"
        self.remote_conf_path = f"{self.remote_root_path}/conf"
        self.remote_data_path = f"{self.remote_root_path}/data"
        self.remote_container_conf_path = "/etc/nosqlbench"
        self.remote_container_data_path = "/var/lib/nosqlbench"

        self.hosts = None

    @property
    def host_count(self):
        return len(self.hosts) if self.hosts is not None else 0

    def set_hosts(self, hosts: list[en.Host]):
        self.hosts = hosts

    def init(self, hosts: list[en.Host]):
        if len(hosts) <= 0:
            raise MissingHostsException

        self.set_hosts(hosts)

    def deploy(self):
        with en.actions(roles=self.hosts) as actions:
            actions.file(path=self.remote_root_path, state="directory")
            actions.file(path=self.remote_conf_path, state="directory")
            actions.file(path=self.remote_data_path, state="directory")

            actions.copy(src=str(self.workloads), dest=self.remote_conf_path)

            actions.docker_image(name=self.docker_image, source="pull")

        logging.info("NoSQLBench has been deployed. Ready to benchmark.")

    def destroy(self):
        with en.actions(roles=self.hosts) as actions:
            actions.file(path=self.remote_root_path, state="absent")

    def command(self, cmd: Union[str, Command], hosts: Optional[list[en.Host]] = None):
        if isinstance(cmd, Command):
            cmd = str(cmd)

        if hosts is None:
            hosts = self.hosts

        logging.info(f"Running command: {cmd}.")

        with en.actions(roles=hosts) as actions:
            actions.docker_container(name=self.name,
                                     image=self.docker_image,
                                     detach="no",
                                     network_mode="host",
                                     mounts=[
                                         {
                                             "source": self.remote_conf_path,
                                             "target": self.remote_container_conf_path,
                                             "type": "bind"
                                         },
                                         {
                                             "source": self.remote_data_path,
                                             "target": self.remote_container_data_path,
                                             "type": "bind"
                                         }
                                     ],
                                     command=cmd,
                                     auto_remove="yes")

    def workload(self, name: str):
        workload_dir = pathlib.Path(self.workloads).name
        return f"{self.remote_container_conf_path}/{workload_dir}/{name}"

    def data(self, path=""):
        return f"{self.remote_container_data_path}{path}"

    def sync_results(self, local_dest: Union[str, pathlib.Path], hosts: Optional[list[en.Host]] = None):
        # Ensure destination folder exists
        pathlib.Path(local_dest).mkdir(parents=True, exist_ok=True)

        if hosts is None:
            hosts = self.hosts

        with en.actions(roles=hosts) as actions:
            actions.synchronize(src=self.remote_data_path, dest=str(local_dest), mode="pull")
