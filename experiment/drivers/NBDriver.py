import logging
from pathlib import Path
from typing import Optional, Union

import enoslib as en

from .Driver import Driver


class Command:
    def __init__(self):
        self.tokens = []

    def token(self, token: str):
        self.tokens.append(token)

        return self

    def __str__(self):
        return " ".join(self.tokens)


class ParameterizedCommand(Command):
    def parameter(self, key, value):
        self.token(f"{key}={value}")

        return self

    def parameters(self, **kwargs):
        for key in kwargs:
            self.parameter(key, kwargs[key])

        return self


class RunCommand(ParameterizedCommand):
    def __init__(self):
        super().__init__()
        self.token("run")

    @staticmethod
    def create(**kwargs):
        return RunCommand().parameters(**kwargs)


class StartCommand(ParameterizedCommand):
    def __init__(self):
        super().__init__()
        self.token("start")

    @staticmethod
    def create(**kwargs):
        return StartCommand().parameters(**kwargs)


class AwaitCommand(Command):
    def __init__(self):
        super().__init__()
        self.token("await")

    @staticmethod
    def create(alias: str):
        return AwaitCommand().token(alias)


class StopCommand(Command):
    def __init__(self):
        super().__init__()
        self.token("stop")

    @staticmethod
    def create(alias: str):
        return StopCommand().token(alias)


class Scenario:
    def __init__(self):
        self.cmds = []
        self.args = []

    @staticmethod
    def create(*commands: Command):
        return Scenario().commands(*commands)

    def commands(self, *commands: Command):
        self.cmds.extend(commands)

        return self

    def arg(self, name, value):
        self.args.append(name)
        self.args.append(str(value))

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
        cmds = " ".join((str(cmd) for cmd in self.cmds))
        args = " ".join(self.args)

        return cmds + " " + args

    def as_string(self):
        return str(self)


class NBDriver(Driver):
    CONTAINER_NAME = "nb"

    def __init__(self, docker_image: str):
        super().__init__()

        self.docker_image = docker_image

    def deploy(self, hosts: list[en.Host]):
        self.set_hosts(hosts)

        self.create_filetree("remote", [
            {"path": "/root/nosqlbench", "tags": ["root"]},
            {"path": "@root/conf", "tags": ["conf"]},
            {"path": "@conf/driver", "tags": ["driver-conf"]},
            {"path": "@conf/workload", "tags": ["workload-conf"]},
            {"path": "@root/data", "tags": ["data"]}
        ]).build(remote=self.hosts)

        self.create_filetree("remote_container", [
            {"path": "/etc/nosqlbench", "tags": ["conf"]},
            {"path": "@conf/driver", "tags": ["driver-conf"]},
            {"path": "@conf/workload", "tags": ["workload-conf"]},
            {"path": "/var/lib/nosqlbench", "tags": ["data"]}
        ])

        self.create_mount_points([
            ("conf", "{{remote_conf_path}}", "{{remote_container_conf_path}}", "bind"),
            ("data", "{{remote_data_path}}", "{{remote_container_data_path}}", "bind")
        ])

        extra_vars = {
            "remote_conf_path": str(self.filetree("remote").path("conf")),
            "remote_data_path": str(self.filetree("remote").path("data")),
            "remote_static_path": str(self.filetree("remote").path("static-data")),
            "remote_container_conf_path": str(self.filetree("remote_container").path("conf")),
            "remote_container_data_path": str(self.filetree("remote_container").path("data"))
        }

        for host in self.hosts:
            host.extra.update(**extra_vars)

        self.login()

        logging.info(f"NoSQLBench has been deployed (hosts={self.host_addresses()}).")

    def destroy(self):
        self.filetree("remote").remove("root", remote=self.hosts)

    def commands(self, commands: list[tuple[en.Host, str]]):
        hosts = []
        for host, command in commands:
            host.extra.update(command=command)
            hosts.append(host)

            logging.info(f"[{host.address}] Running command `{command}`.")

        with en.actions(roles=hosts) as actions:
            actions.docker_container(name=NBDriver.CONTAINER_NAME, image=self.docker_image, detach="no",
                                     network_mode="host", mounts=self.mounts(), command="{{command}}")

            actions.docker_container(name=NBDriver.CONTAINER_NAME, state="absent")

        for host in hosts:
            host.extra.update(command=None)

    def command(self, command: str, host: Optional[en.Host] = None):
        if host is None:
            host = self.hosts[0]

        self.commands([(host, command)])

    def pull_results(self, basepath: Path):
        for host in self.hosts:
            local_data_path = basepath / host.address
            local_data_path.mkdir(parents=True, exist_ok=True)

            host.extra.update(local_data_path=str(local_data_path))

        self.pull(dest="{{local_data_path}}", src="{{remote_data_path}}")

        with en.actions(roles=self.hosts) as actions:
            actions.shell(cmd="rm -rf {{remote_data_path}}/*")
