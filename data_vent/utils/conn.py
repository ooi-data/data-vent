import datetime
from loguru import logger
import json

import zarr
import fsspec
import requests
from requests.adapters import HTTPAdapter
import xarray as xr
import pandas as pd

from data_vent.config import BASE_URL, M2M_PATH
from data_vent.utils.parser import (
    parse_global_range_dataframe,
    parse_param_dict,
)
from data_vent.settings.main import harvest_settings

DEFAULT_TIMEOUT = 5  # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


SESSION = requests.Session()
# No retries! Just throw failures
adapter = HTTPAdapter(
    max_retries=0,
    pool_connections=1000,
    pool_maxsize=1000,
)
# Times out after 15 min if no response is received from server!
timeout_adapter = TimeoutHTTPAdapter(timeout=900)
SESSION.mount("https://", adapter)
SESSION.mount("https://", timeout_adapter)


def check_zarr(dest_fold, storage_options={}):
    fsmap = fsspec.get_mapper(dest_fold, **storage_options)
    if fsmap.get(".zmetadata") is not None:
        zgroup = zarr.open_consolidated(fsmap)
        if "time" not in zgroup:
            raise ValueError(f"Dimension time is missing from the dataset {dest_fold}!")

        time_array = zgroup["time"]
        calendar = time_array.attrs.get("calendar", "gregorian")
        units = time_array.attrs.get("units", "seconds since 1900-01-01 0:0:0")
        last_time = xr.coding.times.decode_cf_datetime(
            [time_array[-1]], units=units, calendar=calendar
        )
        return True, last_time[0]
    else:
        return False, None


def get_global_ranges():
    logger.info("Fetching global ranges ...")
    url = "https://raw.githubusercontent.com/ooi-integration/qc-lookup/master/data_qc_global_range_values.csv"  # noqa
    return parse_global_range_dataframe(pd.read_csv(url))


def get_stream(stream):
    url = f"{BASE_URL}/{M2M_PATH}/12575/stream/byname/{stream}"
    stream_dict = send_request(url)
    return {
        "stream_id": stream_dict["id"],
        "stream_rd": stream,
        "stream_type": stream_dict["stream_type"]["value"],
        "stream_content": stream_dict["stream_content"]["value"],
        "parameters": [parse_param_dict(p) for p in stream_dict["parameters"]],
        "last_updated": datetime.datetime.utcnow().isoformat(),
    }


def fetch_streams(inst):  # instruments
    logger.debug(inst["reference_designator"])
    streams_list = []
    # excepted_streams_list = []
    for stream in inst["streams"]:
        newst = stream.copy()
        try:
            newst.update(get_stream(stream["stream"]))
        except KeyError as e:
            logger.warning(
                f"{e} - request for {stream} may have returned code other than 200:"
            )

        streams_list.append(
            dict(
                reference_designator=inst["reference_designator"],
                platform_code=inst["platform_code"],
                mooring_code=inst["mooring_code"],
                instrument_code=inst["instrument_code"],
                **newst,
            )
        )

    return streams_list


def fetch_url(prepped_request, session=None, stream=False, **kwargs):
    session = session or requests.Session()
    r = session.send(prepped_request, stream=stream, **kwargs)

    if r.status_code == 200:
        logger.debug(f"URL fetch {prepped_request.url} successful.")
        return r
    elif r.status_code == 500:
        message = "Server is currently down."
        if "ooinet.oceanobservatories.org/api" in prepped_request.url:
            message = "UFrame M2M is currently down."
        logger.warning(message)
        return r
    else:
        message = f"Request {prepped_request.url} failed: {r.status_code}, {r.reason}"
        logger.warning(message)  # noqa
        return r


def send_request(url, params=None, username=None, token=None):
    """Send request to OOI. Username and Token already included."""
    if username is None:
        # When not provided, grab username from settings
        username = harvest_settings.ooi_config.username

    if token is None:
        # When not provided, grab token from settings
        token = harvest_settings.ooi_config.token

    if username is None and token is None:
        raise ValueError("Please provide ooi username and token!")
    try:
        prepped_request = requests.Request(
            "GET", url, params=params, auth=(username, token)
        ).prepare()
        request_dt = datetime.datetime.utcnow().isoformat()
        r = fetch_url(prepped_request, session=SESSION)
        if r.status_code == 200:
            result = r.json()
        else:
            result = {"status_code": r.status_code, "reason": r.reason}
        result.setdefault("request_dt", request_dt)
        return result
    except json.JSONDecodeError as e:
        logger.warning(e)
        return None


def get_toc():  # get table of contents
    url = f"{BASE_URL}/{M2M_PATH}/12576/sensor/inv/toc"
    return send_request(url)


def request_data(
    platform,
    mooring,
    instrument,
    stream_method,
    stream,
    start_dt,
    end_dt,
    output_format="application/netcdf",
    limit=-1,
    exec_dpa=True,
    provenance=False,
    email=None,
    estimate=False,
):
    url = f"{BASE_URL}/{M2M_PATH}/12576/sensor/inv/{platform}/{mooring}/{instrument}/{stream_method}/{stream}"
    params = {
        "beginDT": start_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "endDT": end_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "format": output_format,
        "limit": limit,
        "execDPA": str(exec_dpa).lower(),
        "include_provenance": str(provenance).lower(),
        "estimate_only": str(estimate).lower(),
        "email": str(email),
    }
    return send_request(url, params), {"url": url, "params": params}
