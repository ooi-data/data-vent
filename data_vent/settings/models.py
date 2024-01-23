import os
from pydantic import BaseModel, Field, PyObject, validator, field_validator
from data_vent.utils.core import prefect_version
from pydantic_settings import BaseSettings

PREFECT_VERSION = prefect_version()


def get_prefect_secret(key):
    if PREFECT_VERSION < (2, 0, 0):
        import prefect

        prefect_secret = prefect.client.Secret(key)
        if prefect_secret.exists():
            return prefect_secret.get()
        return None
    return os.environ.get(key, None)


class GithubStatusDefaults(BaseModel):
    status_emojis: dict = {
        "pending": "🔵",
        "failed": "🔴",
        "success": "🟢",
        "skip": "🟠",
        "discontinued": "⚫️",
    }
    process_commit_message_template: PyObject = (
        "{status_emoji} Data processing [{status}] ({request_dt})".format
    )
    commit_message_template: PyObject = (
        "{status_emoji} Data request [{status}] ({request_dt})".format
    )
    config_path_str: str = "config.yaml"
    process_status_path_str: str = "history/process.yaml"
    request_status_path_str: str = "history/request.yaml"
    response_path_str: str = "history/response.json"


class GithubConfig(BaseSettings):
    defaults: GithubStatusDefaults = GithubStatusDefaults()
    main_branch: str = Field("main")
    data_org: str = Field("ooi-data")
    pat: str = Field(None, validation_alias="gh_pat")

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    #@validator("pat", pre=True, always=True)
    @field_validator("pat")
    def pat_prefect(cls, v):
        if v is None:
            return get_prefect_secret("GH_PAT")
        return v


class AWSConfig(BaseSettings):
    key: str = Field(None, validation_alias="aws_key")
    secret: str = Field(None, validation_alias="aws_secret")

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    #@validator("key", pre=True, always=True)
    @field_validator("key")
    def key_prefect(cls, v):
        if v is None:
            return get_prefect_secret("AWS_KEY")
        return v

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    # @validator("secret", pre=True, always=True)
    @field_validator("secret")
    def secret_prefect(cls, v):
        if v is None:
            return get_prefect_secret("AWS_KEY")
        return v


class S3Buckets(BaseModel):
    metadata: str = "ooi-metadata-prod"
    harvest_cache: str = "flow-process-bucket"

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    #@validator("*", pre=True, always=True)
    @field_validator("*")
    def add_s3(cls, v):
        if "s3://" not in v:
            return f"s3://{v}"
        return v


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

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    #@validator("username", pre=True, always=True)
    @field_validator("username")
    def username_prefect(cls, v):
        if v is None:
            return get_prefect_secret("OOI_USERNAME")
        return v

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    #@validator("token", pre=True, always=True)
    @field_validator("token")
    def token_prefect(cls, v):
        if v is None:
            return get_prefect_secret("OOI_TOKEN")
        return v


class StorageOptions(BaseSettings):
    aws: AWSConfig = AWSConfig()
