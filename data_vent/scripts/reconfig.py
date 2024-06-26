from typing import Any
import os
import fsspec
from loguru import logger
import yaml
from pathlib import Path


def change_config_param(new_config_param: Any):
    config_dir = os.path.join(os.getcwd(), "flow_configs")
    fs = fsspec.filesystem("")
    glob_path = config_dir + "/**/*.yaml"
    logger.info(f"Searching for config yamls at path: {glob_path}")
    all_paths = fs.glob(glob_path)

    for path in all_paths:
        config_json = yaml.safe_load(Path(path).open())
        if config_json["harvest_options"]["path"] != new_config_param:
            config_json["harvest_options"]["path"] = new_config_param

            with open(path, "w") as file:
                yaml.safe_dump(config_json, file)
            logger.info("param changed!")
        else:
            logger.info("no change needed")


if __name__ == "__main__":
    change_config_param(new_config_param="s3://ooi-data")
