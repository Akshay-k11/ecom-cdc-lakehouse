"""Read pipeline settings from YAML files in conf/."""

import yaml


def load_tables(path:str):
    """Return the {table name: CSV file name} mapping from a sources YAML file."""
    with open(path) as f:
        config = yaml.safe_load(f)

    return config["tables"]
