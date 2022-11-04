import pathlib
from typing import Union

import yaml


def update_dict_from_spec(data: Union[dict, list], update_spec: dict):
    """
    Update a dict (or a list) according to a given update specification.
    """

    for key in update_spec:
        if isinstance(update_spec[key], dict):
            update_dict_from_spec(data[key], update_spec[key])
        else:
            data[key] = update_spec[key]


def build_yaml(template_path: Union[str, pathlib.Path], output_path: Union[str, pathlib.Path], update_spec: dict):
    """
    Load a YAML template file, update some properties, and write the result
    to a new YAML file.

    This function takes an update specification to inject the modified values
    at the right location in the YAML structure. This specification takes the
    form of a Python dictionary that follows the structure of the property to
    update.

    This is easier to understand with an example. Let us say we have the
    following YAML template file:

    ```yaml
    foo:
      bar:
        baz: 0
        qux: 1
    ```

    Then, suppose we apply the following update specification:

    ```python
    spec = {
        "foo": {
            "bar": {
                "qux": 1234
            }
        }
    }
    ```

    The resulting YAML file will be:

    ```yaml
    foo:
      bar:
        baz: 0
        qux: 1234
    ```

    Note that the other property `bar` has not been modified.
    """

    with pathlib.Path(template_path).open("r") as template_file:
        data = yaml.safe_load(template_file)

    update_dict_from_spec(data, update_spec)

    with pathlib.Path(output_path).open("w") as output_file:
        yaml.dump(data, output_file)
