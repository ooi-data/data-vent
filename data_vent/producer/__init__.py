import os
import json
import datetime
from typing import Optional
import textwrap

import fsspec
from loguru import logger
from siphon.catalog import TDSCatalog
import numpy as np
from dateutil import parser
from uuid import uuid4
from prefect import get_run_logger

from data_vent.config import HARVEST_CACHE_BUCKET, OOI_EMAIL
from data_vent.utils.conn import request_data, check_zarr, send_request
from data_vent.utils.parser import estimate_size_and_time, parse_uframe_response

from data_vent.utils.compute import map_concurrency
from data_vent.metadata.fetcher import fetch_instrument_streams_list

# from ooi_harvester.metadata.utils import get_catalog_meta
from data_vent.utils.conn import get_toc
from data_vent.metadata import get_ooi_streams_and_parameters
from data_vent.producer.models import StreamHarvest
from data_vent.utils.parser import (
    filter_ooi_datasets,
    parse_ooi_data_catalog,
)

BASE_THREDDS = "https://opendap-west.oceanobservatories.org/thredds/catalog/ooi"
BASE_ASYNC = "https://downloads-west.oceanobservatories.org/async_results"


def fetch_streams_list(stream_harvest: StreamHarvest) -> list:
    instruments = get_toc()["instruments"]
    filtered_instruments = [
        i for i in instruments if i["reference_designator"] == stream_harvest.instrument
    ]
    streams_df, _ = get_ooi_streams_and_parameters(filtered_instruments)
    # Only get science stream
    # streams_json = streams_df[
    #     streams_df.stream_type.str.match('Science')
    # ].to_json(orient='records')
    # 09/16/2022 Don.S.: Change to not filter only Science stream
    #                    this should be done upstream.
    streams_json = streams_df.to_json(orient="records")
    streams_list = json.loads(streams_json)
    return streams_list


# def request_axiom_catalog(stream_dct):
#     axiom_ooi_catalog = TDSCatalog(
#         'http://thredds.dataexplorer.oceanobservatories.org/thredds/catalog/ooigoldcopy/public/catalog.xml'  # noqa
#     )
#     stream_name = stream_dct['table_name']
#     ref = axiom_ooi_catalog.catalog_refs[stream_name]
#     catalog_dict = get_catalog_meta((stream_name, ref))
#     return catalog_dict


# def create_catalog_request(
#     stream_dct: dict,
#     start_dt: Optional[str] = None,
#     end_dt: Optional[str] = None,
#     refresh: bool = False,
#     existing_data_path: Optional[str] = None,
#     client_kwargs: dict = {},
# ):
#     """Creates a catalog request to the gold copy"""
#     beginTime = np.datetime64(parser.parse(stream_dct['beginTime']))
#     endTime = np.datetime64(parser.parse(stream_dct['endTime']))

#     filter_ds = False
#     zarr_exists = False
#     if not refresh:
#         if existing_data_path is not None:
#             storage_options = dict(
#                 client_kwargs=client_kwargs,
#                 **get_storage_options(existing_data_path),
#             )
#             zarr_exists, last_time = check_zarr(
#                 os.path.join(existing_data_path, stream_dct['table_name']),
#                 storage_options,
#             )
#         else:
#             raise ValueError(
#                 "Please provide existing data path when not refreshing."
#             )

#     if zarr_exists:
#         start_dt = last_time
#         end_dt = np.datetime64(datetime.datetime.utcnow())

#     if start_dt:
#         beginTime = (
#             np.datetime64(start_dt) if isinstance(start_dt, str) else start_dt
#         )
#         filter_ds = True
#     if end_dt:
#         endTime = np.datetime64(end_dt) if isinstance(end_dt, str) else end_dt
#         filter_ds = True

#     catalog_dict = request_axiom_catalog(stream_dct)
#     filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)

#     if not refresh or filter_ds:
#         filtered_datasets = filter_datasets_by_time(
#             filtered_catalog_dict['datasets'], beginTime, endTime
#         )
#         total_bytes = np.sum([d['size_bytes'] for d in filtered_datasets])
#         deployments = list({d['deployment'] for d in filtered_datasets})
#         filtered_provenance = [
#             p
#             for p in filtered_catalog_dict['provenance']
#             if p['deployment'] in deployments
#         ]
#         filtered_catalog_dict.update(
#             {
#                 'datasets': filtered_datasets,
#                 'provenance': filtered_provenance,
#                 'total_data_size': memory_repr(total_bytes),
#                 'total_data_bytes': total_bytes,
#             }
#         )

#     download_cat = os.path.join(
#         catalog_dict['base_tds_url'],
#         'thredds/fileServer/ooigoldcopy/public',
#         catalog_dict['stream_name'],
#     )
#     result_dict = {
#         'thredds_catalog': catalog_dict['catalog_url'],
#         'download_catalog': download_cat,
#         'status_url': os.path.join(download_cat, 'status.txt'),
#         'request_dt': catalog_dict['retrieved_dt'],
#         'data_size': filtered_catalog_dict['total_data_bytes'],
#         'units': {'data_size': 'bytes', 'request_dt': 'UTC'},
#     }
#     return {
#         "stream_name": catalog_dict["stream_name"],
#         "catalog_url": catalog_dict["catalog_url"],
#         "base_tds_url": catalog_dict["base_tds_url"],
#         "async_url": download_cat,
#         "result": result_dict,
#         "stream": stream_dct,
#         "zarr_exists": zarr_exists,
#         "datasets": filtered_catalog_dict['datasets'],
#         "provenance": filtered_catalog_dict['provenance'],
#         "params": {
#             "beginDT": str(beginTime),
#             "endDT": str(endTime),
#             "include_provenance": True,
#         },
#     }


def create_request_estimate(
    stream_dct: dict,
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
    refresh: bool = False,
    existing_data_path: str = None,
    request_kwargs: dict = {},
    storage_options: dict = {},
):
    """Creates an estimated request to OOI M2M"""
    logger = get_run_logger()
    beginTime = np.datetime64(parser.parse(stream_dct["beginTime"]))
    endTime = np.datetime64(parser.parse(stream_dct["endTime"]))

    zarr_exists = False
    if not refresh:
        if existing_data_path is not None:
            zarr_exists, last_time = check_zarr(
                os.path.join(existing_data_path, stream_dct["table_name"]),
                storage_options=storage_options,
            )
        else:
            raise ValueError("Please provide existing data path when not refreshing.")

    if zarr_exists:
        start_dt = last_time
        end_dt = np.datetime64(datetime.datetime.utcnow())

    if start_dt is not None:
        if isinstance(start_dt, str):
            beginTime = np.datetime64(start_dt)
        elif isinstance(start_dt, np.datetime64):
            beginTime = start_dt
        else:  ## string and np.datetime64 could be causing trouble?
            raise TypeError("start_dt must be a string or np.datetime64!")
    if end_dt is not None:
        if isinstance(end_dt, str):
            endTime = np.datetime64(end_dt)
        elif isinstance(end_dt, np.datetime64):
            endTime = end_dt
        else:
            raise TypeError("end_dt must be a string or np.datetime64!")

    # form the actual request dictionary
    response, request_dict = request_data(
        stream_dct["platform_code"],
        stream_dct["mooring_code"],
        stream_dct["instrument_code"],
        stream_dct["method"],
        stream_dct["stream"],
        parser.parse(str(beginTime)),
        parser.parse(str(endTime)),
        estimate=True,
        **request_kwargs,
    )
    if response:
        table_name = f"{stream_dct['reference_designator']}-{stream_dct['method']}-{stream_dct['stream']}"  # noqa
        text = textwrap.dedent(
            """\
        *************************************
        {0}
        -------------------------------------
        {1}
        *************************************
        """
        ).format
        if "requestUUID" in response:
            m = estimate_size_and_time(response)
            request_dict["params"].update({"estimate_only": "false"})
            request_dict.update(
                {
                    "stream": stream_dct,
                    "estimated": response,
                    "zarr_exists": zarr_exists,
                }
            )
            logger.debug(text(table_name, m))
        else:
            m = "Skipping... Data not available."
            request_dict.update({"stream": stream_dct, "estimated": response})
            logger.debug(text(table_name, m))

        return request_dict


def _sort_and_filter_estimated_requests(estimated_requests):
    """Internal sorting function"""

    success_requests = sorted(
        filter(lambda req: "requestUUID" in req["estimated"], estimated_requests),
        key=lambda i: i["stream"]["count"],
    )
    failed_requests = list(
        filter(
            lambda req: "requestUUID" not in req["estimated"],
            estimated_requests,
        )
    )

    return {
        "success_requests": success_requests,
        "failed_requests": failed_requests,
    }


def check_data_catalog_readiness(datasets):
    try:
        next(filter(lambda d: "status.txt" in d["name"], datasets))
        return True
    except StopIteration:
        return False


def check_thredds_cache(stream_name: str):
    """
    Parameters
    ----------
    stream_name : str
        stream table name
    """
    ooi_email = os.environ.get("OOI_EMAIL", OOI_EMAIL)
    catalog = TDSCatalog(f"{BASE_THREDDS}/{ooi_email}/catalog.html".replace(".html", ".xml"))
    ref_keys = sorted(
        [ref for ref in catalog.catalog_refs if stream_name in ref], reverse=True
    )
    catalog_refs = {}
    result = None
    for ref in ref_keys:
        cat = catalog.catalog_refs[ref]
        catalog_dict = parse_ooi_data_catalog(cat.href)
        catalog_refs[ref] = catalog_dict
        datasets = catalog_dict["datasets"]
        ready = check_data_catalog_readiness(datasets)
        if ready:
            _, datasets = filter_ooi_datasets(datasets, stream_name)
            if len(datasets) > 0:
                data_range = (
                    parser.parse(datasets[-1]["start_ts"]),
                    parser.parse(datasets[0]["end_ts"]),
                )
                data_timedelta = data_range[-1] - data_range[0]
                # If the amount of data is greater than 90 days
                # use it!
                if data_timedelta.days > 90:
                    result = {
                        "request_id": f"cache-{str(uuid4())}",
                        "thredds_catalog": cat.href,
                        "download_catalog": f"{BASE_ASYNC}/{ooi_email}/{cat.title}",
                        "status_url": f"{BASE_ASYNC}/{ooi_email}/{cat.title}/status.txt",
                        "data_size": sum(d["size_bytes"] for d in datasets),
                        "estimated_time": 0,
                        "units": {
                            "data_size": "bytes",
                            "estimated_time": "seconds",
                            "request_dt": "UTC",
                        },
                        "request_dt": datetime.datetime.utcnow().isoformat(),
                    }
                    break
    return result


def perform_request(req, refresh=False, logger=logger, storage_options={}, force=False):
    """
    req : dict
        GET request sent to OOI m2m API requesting data for specified reference designator,
        for specified time period.
    """
    TODAY_DATE = datetime.datetime.utcnow()
    name = req["stream"]["table_name"]

    refresh_text = "refresh" if refresh else "daily"
    datestr = f"{TODAY_DATE:%Y%m}" if refresh else f"{TODAY_DATE:%Y%m%dT%H%M}"
    fname = f"{name}__{datestr}__{refresh_text}"
    fs = fsspec.filesystem(
        HARVEST_CACHE_BUCKET.split(":")[0],
        **storage_options,
    )
    fpath = os.path.join(HARVEST_CACHE_BUCKET, "ooinet-requests", fname)

    if fs.exists(fpath) and not force:
        logger.info(f"Already requested {name} on {datestr} for {refresh_text} ({fpath})")
        with fs.open(fpath, mode="r") as f:
            response = json.load(f)
    else:
        result = None
        if refresh and not force:
            # Only check thredds during refresh!
            result = check_thredds_cache(name)

        if result is None:
            logger.info(f"Requesting {name}")
            result = parse_uframe_response(send_request(req["url"], params=req["params"]))
        else:
            logger.info("Cache found in OOI Thredds, using those!")

        response = dict(
            result=result,
            **req,
        )
        with fs.open(fpath, mode="w") as f:
            json.dump(response, f)

    response.setdefault("file_path", fpath)
    return response


def perform_estimates(instrument_rd, refresh, existing_data_path):
    streams_list = fetch_instrument_streams_list(instrument_rd)
    estimated_requests = map_concurrency(
        create_request_estimate,
        streams_list,
        func_kwargs=dict(
            refresh=refresh,
            existing_data_path=existing_data_path,
            request_kwargs=dict(provenance=True),
        ),
        max_workers=50,
    )
    estimated_dict = _sort_and_filter_estimated_requests(estimated_requests)
    success_requests = estimated_dict["success_requests"]
    return success_requests


def fetch_harvest(instrument_rd, refresh, existing_data_path):
    success_requests = perform_estimates(instrument_rd, refresh, existing_data_path)
    request_responses = []
    if len(success_requests) > 0:
        request_responses = [perform_request(req, refresh) for req in success_requests]
    return request_responses
