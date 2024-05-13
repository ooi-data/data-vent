import os
from pydantic import BaseSettings, BaseModel, Field, PyObject, validator
from data_vent.utils.core import prefect_version

PREFECT_VERSION = prefect_version()


def get_prefect_secret(key):
    if PREFECT_VERSION < (2, 0, 0):
        import prefect

        prefect_secret = prefect.client.Secret(key)
        if prefect_secret.exists():
            return prefect_secret.get()
        return None
    return os.environ.get(key, None)


class GithubConfig(BaseSettings):
    main_branch: str = Field("main")
    data_org: str = Field("ooi-data")
    pat: str = Field(None, env="gh_pat")

    @validator("pat", pre=True, always=True)
    def pat_prefect(cls, v):
        if v is None:
            return get_prefect_secret("GH_PAT")
        return v


class AWSConfig(BaseSettings):
    key: str = Field(None, env="aws_key")
    secret: str = Field(None, env="aws_secret")

    @validator("key", pre=True, always=True)
    def key_prefect(cls, v):
        if v is None:
            return get_prefect_secret("AWS_KEY")
        return v

    @validator("secret", pre=True, always=True)
    def secret_prefect(cls, v):
        if v is None:
            return get_prefect_secret("AWS_KEY")
        return v


class S3Buckets(BaseModel):
    metadata: str = "ooi-metadata-prod"
    harvest_cache: str = "flow-process-bucket"

    @validator("*", pre=True, always=True)
    def add_s3(cls, v):
        if "s3://" not in v:
            return f"s3://{v}"
        return v


class OOIConfig(BaseSettings):
    username: str = Field(None, env="ooi_username")
    token: str = Field(None, env="ooi_token")
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

    @validator("username", pre=True, always=True)
    def username_prefect(cls, v):
        if v is None:
            return get_prefect_secret("OOI_USERNAME")
        return v

    @validator("token", pre=True, always=True)
    def token_prefect(cls, v):
        if v is None:
            return get_prefect_secret("OOI_TOKEN")
        return v


class StorageOptions(BaseSettings):
    aws: AWSConfig = AWSConfig()
