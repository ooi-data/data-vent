from typing import List, Literal, Optional, Dict, Any
from datetime import datetime
from copy import deepcopy

from pydantic import field_validator, BaseModel


class Stream(BaseModel):
    method: str
    name: str

    @field_validator("*")
    @classmethod
    def must_exists(cls, v):
        if not v.strip():
            raise ValueError("Stream method or name cannot be empty")
        return v


class HarvestRange(BaseModel):
    start: Optional[str] = None
    end: Optional[str] = None

    @field_validator("start")
    @classmethod
    def start_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                "start custom range is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)"
            )

    @field_validator("end")
    @classmethod
    def end_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                "end custom range is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSSZ)"
            )


class HarvestOptions(BaseModel):
    path: str
    force_harvest: bool = False
    refresh: bool = False
    test: bool = False
    goldcopy: bool = False
    path_settings: dict = {}
    custom_range: HarvestRange = HarvestRange()

    @field_validator("path")
    @classmethod
    def path_must_exists(cls, v):
        if not v.strip():
            raise ValueError("Harvest destination path cannot be empty")
        return v


class Workflow(BaseModel):
    schedule: str


class HarvestStatus(BaseModel):
    status: Literal[
        "started", "pending", "failed", "success", "discontinued", "unknown"
    ] = "unknown"
    last_updated: Optional[str] = None
    # OOI Data request information
    data_ready: bool = False
    data_response: Optional[str] = None
    requested_at: Optional[str] = None
    # Cloud data information
    process_status: Optional[Literal["pending", "failed", "success"]] = None
    cloud_location: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    processed_at: Optional[str] = None
    last_refresh: Optional[str] = None
    data_check: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        self.last_updated = datetime.utcnow().isoformat()

    @field_validator("processed_at")
    @classmethod
    def processed_at_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                "processed_at is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)"
            )

    @field_validator("last_refresh")
    @classmethod
    def last_refresh_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                "last_refresh is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)"
            )

    @field_validator("requested_at")
    @classmethod
    def requested_at_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                "requested_at is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)"
            )


class StreamHarvest(BaseModel):
    instrument: str
    stream: Stream
    assignees: Optional[List[str]] = None
    labels: Optional[List[str]] = None
    harvest_options: HarvestOptions
    workflow_config: Workflow
    table_name: Optional[str] = None
    _status: HarvestStatus

    def __init__(self, **data):
        super().__init__(**data)
        self.table_name = "-".join([self.instrument, self.stream.method, self.stream.name])
        self._status = HarvestStatus()

    @property
    def status(self):
        return self._status

    def update_status(self, status_input: Dict[str, Any]):
        new_status = deepcopy(self._status.dict())
        new_status.update(status_input)
        self._status = HarvestStatus(**new_status)

    @field_validator("instrument")
    @classmethod
    def instrument_must_exists(cls, v):
        if not v.strip():
            raise ValueError("Instrument cannot be empty")
        return v

