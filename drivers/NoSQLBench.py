import logging
import pathlib
from typing import Optional, Union

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
    def __init__(self, name: str, docker_image: str, driver_path: Union[str, pathlib.Path],
                 workload_path: Union[str, pathlib.Path]):
        self.name = name
        self.docker_image = docker_image
        self.driver_path = driver_path
        self.workload_path = workload_path

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

            actions.copy(src=str(self.driver_path), dest=self.remote_conf_path)
            actions.copy(src=str(self.workload_path), dest=self.remote_conf_path)

            actions.docker_image(name=self.docker_image, source="pull")

        logging.info("NoSQLBench has been deployed. Ready to benchmark.")

    def destroy(self):
        with en.actions(roles=self.hosts) as actions:
            actions.file(path=self.remote_root_path, state="absent")

    def command(self, name: str, cmds: list[tuple[en.Host, Union[str, Command]]]):
        hosts = []

        for host, cmd in cmds:
            if isinstance(cmd, Command):
                cmd = str(cmd)

            host.extra.update(current_command=cmd)
            hosts.append(host)

            logging.info(f"[{host.address}] Running command `{cmd}`.")

        with en.actions(roles=hosts) as actions:
            actions.docker_container(name=name,
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
                                     command="{{current_command}}")

            actions.docker_container(name=name, state="absent")

        for host in hosts:
            host.extra.update(current_command=None)

    def single_command(self, name: str, cmd: Union[str, Command], host: Optional[en.Host] = None):
        if host is None:
            host = self.hosts[0]

        self.command(name, [(host, cmd)])

    def driver(self, name: str):
        driver_dir = pathlib.Path(self.driver_path).name
        return f"{self.remote_container_conf_path}/{driver_dir}/{name}"

    def workload(self, name: str):
        workload_dir = pathlib.Path(self.workload_path).name
        return f"{self.remote_container_conf_path}/{workload_dir}/{name}"

    def data(self, path=""):
        return f"{self.remote_container_data_path}{path}"

    def sync_results(self, basepath: Union[str, pathlib.Path], hosts: Optional[list[en.Host]] = None):
        if isinstance(basepath, str):
            basepath = pathlib.Path(basepath)

        # Ensure dest folder exists
        basepath.mkdir(parents=True, exist_ok=True)

        if hosts is None:
            hosts = self.hosts

        for host in hosts:
            local_path = basepath / host.address
            local_path.mkdir(parents=True, exist_ok=True)

            host.extra.update(local_path=str(local_path))

        with en.actions(roles=hosts) as actions:
            actions.synchronize(src=self.remote_data_path, dest="{{local_path}}", mode="pull")
