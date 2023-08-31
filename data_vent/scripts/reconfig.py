import json
import os
import fsspec
from loguru import logger


config_dir = os.path.join(os.getcwd(), "flow_configs")
fs = fsspec.filesystem('')
glob_path = config_dir + '/**/*.yaml'   
logger.info(f"Searching for config yamls at path: {glob_path}")
all_paths = fs.glob(glob_path)