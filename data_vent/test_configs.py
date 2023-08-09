from typing import Optional, Dict, Any
import yaml
import os
from pathlib import Path

from pydantic import BaseModel

FLOW_PROCESS_BUCKET = 'io2data-harvest-cache'
DEV_PATH_SETTINGS = {"aws":{
    'client_kwargs':{'endpoint_url':'http://127.0.0.1:9000'},
    'key':'admin',
    'secret':'password'
}}

class FlowParameters(BaseModel):
    config: Optional[Dict[str, Any]]
    target_bucket: str = "s3://ooi-data"
    max_chunk: str = "100MB"
    export_da: bool = False
    gh_write_da: bool = False
    error_test: bool = False

config_path = os.path.join(os.getcwd(), 'data_vent', 'stream_configs', 'CE01ISSM-MFD35-01-VEL3DD000-telemetered-vel3d_cd_dcl_velocity_data.yaml')
config_json = yaml.safe_load(Path(config_path).open())

my_params = {
    'config': config_json,
    'target_bucket': "s3://ooi-data",
    'max_chunk': "100MB",
    'export_da': False,
    'gh_wrtie_da': False,
    'error_test': False,
}
