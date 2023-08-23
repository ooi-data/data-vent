import os
import fsspec
import glob
import yaml

from pathlib import Path
from prefect import flow, get_run_logger
from data_vent.flow import stream_ingest



@flow
def run_stream_ingest(test_run: bool=True):

    logger = get_run_logger()
    logger.info("Starting parent flow...")

    config_dir = os.path.join(os.getcwd(), "flow_configs")

    fs = fsspec.filesystem('')
    glob_path = config_dir + '/**/*.yaml'

    logger.info(f"Searching for config yamls at path: {glob_path}")
    all_paths = fs.glob(glob_path)

    if test_run:
        all_paths = all_paths[:3]
        logger.info(all_paths)

    for config_path in all_paths:
        config_json = yaml.safe_load(Path(config_path).open())
        run_name = "-".join(
            [
                config_json['instrument'],
                config_json['stream']['method'],
                config_json['stream']['name'],
            ]
        )
        logger.info(f"=== {run_name} ===")

    logger.warning("this is a test")



if __name__ == '__main__':
    run_stream_ingest()