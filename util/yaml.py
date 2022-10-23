from pathlib import Path
from typing import Union

from yaml import safe_load, dump


def _update(yaml_dict: Union[dict, list], update_spec: dict):
    for key in update_spec:
        if isinstance(update_spec[key], dict):
            _update(yaml_dict[key], update_spec[key])
        else:
            yaml_dict[key] = update_spec[key]


class YamlConfig:
    def __init__(self, yaml_dict=None):
        if yaml_dict is None:
            yaml_dict = {}

        self.yaml_dict = yaml_dict

    @staticmethod
    def from_file(file_path: Union[str, Path]):
        with Path(file_path).open("r") as file:
            yaml_dict = safe_load(file)

        return YamlConfig(yaml_dict)

    def update(self, spec):
        _update(self.yaml_dict, spec)

    def to_yaml(self, file_path: Union[str, Path]):
        with Path(file_path).open("w") as file:
            dump(self.yaml_dict, file)
