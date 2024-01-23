from data_vent.settings.models import (
    StorageOptions,
    OOIConfig,
    S3Buckets,
    GithubConfig,
)
from pydantic_settings import BaseSettings


class HarvestSettings(BaseSettings):
    storage_options: StorageOptions = StorageOptions()
    s3_buckets: S3Buckets = S3Buckets()
    ooi_config: OOIConfig = OOIConfig()
    github: GithubConfig = GithubConfig()


harvest_settings = HarvestSettings()
