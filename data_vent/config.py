from data_vent.settings.main import harvest_settings

# OOI Config
OOI_USERNAME = harvest_settings.ooi_config.username
OOI_TOKEN = harvest_settings.ooi_config.token
BASE_URL = harvest_settings.ooi_config.base_urls.get("ooinet")
M2M_PATH = harvest_settings.ooi_config.paths.get("m2m")
OOI_EMAIL = "jduprey@uw.edu"

# OOI Raw Config
RAW_BASE_URL = harvest_settings.ooi_config.base_urls.get("raw")
RAW_PATH = harvest_settings.ooi_config.paths.get("raw")

# Storage Options
DATA_BUCKET = "ooi-data"
TEMP_DATA_BUCKET = "temp-ooi-data-prod"
STORAGE_OPTIONS = harvest_settings.storage_options.model_dump()
METADATA_BUCKET = harvest_settings.s3_buckets.metadata
HARVEST_CACHE_BUCKET = harvest_settings.s3_buckets.harvest_cache
FLOW_PROCESS_BUCKET = "flow-process-bucket"

# Github
GH_PAT = harvest_settings.github.pat
GH_DATA_ORG = harvest_settings.github.data_org
GH_MAIN_BRANCH = harvest_settings.github.main_branch

# Cloud config
COMPUTE_EXCEPTIONS = {
    "CE04OSBP-LJ01C-07-VEL3DC107-streamed-vel3d_cd_velocity_data": {
        "refresh": "4vcpu_30gb",
        "append": "4vcpu_30gb",
    },
    "CE02SHBP-LJ01D-07-VEL3DC108-streamed-vel3d_cd_velocity_data": {
        "refresh": "4vcpu_30gb",
        "append": "4vcpu_30gb",
    },
}
