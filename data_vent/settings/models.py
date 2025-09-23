import os
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


def get_env_secret(key):
    return os.environ.get(key, None)


class GithubConfig(BaseSettings):
    main_branch: str = Field("main")
    data_org: str = Field("ooi-data")
    pat: str = Field(None, validation_alias="gh_pat")


class AWSConfig(BaseSettings):
    key: str = Field(None, validation_alias="aws_key")
    secret: str = Field(None, validation_alias="aws_secret")
    client_kwargs: dict = {"region_name": "us-west-2"}


class S3Buckets(BaseModel):
    metadata: str = "s3://ooi-metadata-prod"
    harvest_cache: str = "s3://flow-process-bucket"


class OOIConfig(BaseSettings):
    username: str = Field(None, validation_alias="ooi_username")
    token: str = Field(None, validation_alias="ooi_token")
    base_urls: dict = {
        "ooinet": "https://ooinet.oceanobservatories.org",
        "rawdata": "https://rawdata.oceanobservatories.org",
        "cava": "https://api.interactiveoceans.washington.edu",
    }
    paths: dict = {"m2m": "api/m2m", "raw": "files"}
    time: dict = {
        "units": "seconds since 1900-01-01 0:0:0",
        "calendar": "gregorian",
    }


class StorageOptions(BaseSettings):
    aws: AWSConfig = AWSConfig()
