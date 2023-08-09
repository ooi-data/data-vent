import datetime
from typing import Any, Dict
from unittest import result
import xarray as xr
import dask
import json
import time
import tempfile
import dateutil
import fsspec
import zarr
import numpy as np
import dask.array as da
from dateutil import parser

import prefect
from prefect import task, get_run_logger
from prefect.states import Completed, Cancelled, Failed

# TODO sort out imports and __init__ structure
from data_vent.producer import (
    StreamHarvest,
    fetch_streams_list,
    create_request_estimate,
    perform_request,
)

from data_vent.processor.checker import check_in_progress

from data_vent.utils.parser import (
    parse_exception,
    parse_response_thredds,
    filter_and_parse_datasets,
    #setup_etl,
)


from data_vent.test_configs import DEV_PATH_SETTINGS, FLOW_PROCESS_BUCKET

def setup_status_s3fs(
    stream_harvest: StreamHarvest,
):
    fs = fsspec.filesystem(
        's3', **DEV_PATH_SETTINGS["aws"] #**stream_harvest.harvest_options.path_settings
    )
    status_file = (
        f"{FLOW_PROCESS_BUCKET}/harvest-status/{stream_harvest.table_name}"
    )

    return fs, status_file


def write_status_json(
    stream_harvest: StreamHarvest,
):
    fs, status_file = setup_status_s3fs(stream_harvest)
    status_json = stream_harvest.status.dict()
    # write status file from s3 if exists
    with fs.open(status_file, mode='w') as f:
        json.dump(status_json, f)


def read_status_json(
    stream_harvest: StreamHarvest,
):
    fs, status_file = setup_status_s3fs(stream_harvest)
    if fs.exists(status_file):
        # Open status file from s3 if exists
        with fs.open(status_file) as f:
            status_json = json.load(f)

        # update status with status file
        stream_harvest.update_status(status_json)
    return stream_harvest


def update_and_write_status(
    stream_harvest: StreamHarvest,
    status_json: Dict[str, Any],
    write: bool = True
) -> StreamHarvest:
    """
    Update the StreamHarvest object status attribute,
    then write the status json to S3

    Parameters
    ----------
    stream_harvest : StreamHarvest
        The current StreamHarvest object to be modified
    status_json : dict
        Status dictionary to update with
    write : bool
        Flag to execute the S3 json writing or not
    """
    stream_harvest.update_status(status_json)
    if write:
        write_status_json(stream_harvest)
    return stream_harvest


@task
def get_stream_harvest(
    config_json: Dict[str, Any], harvest_options: Dict[str, Any] = {}
):
    logger = get_run_logger()
    config_json['harvest_options'].update(harvest_options)
    stream_harvest = StreamHarvest(**config_json)
    stream_harvest = read_status_json(stream_harvest)
    if stream_harvest.status.last_refresh is not None:
        logger.info(
            f"Cloud data last refreshed on {stream_harvest.status.last_refresh}"
        )
        # 11/28/2022 Don.S: Comment out this section to prevent auto refresh.
        # refresh = harvest_options.get('refresh', None)
        # last_refresh = parser.parse(stream_harvest.status.last_refresh)
        # current_dt = datetime.datetime.utcnow()
        # if (current_dt - last_refresh) < datetime.timedelta(days=30):
        #     if refresh is None:
        #         stream_harvest.harvest_options.refresh = False
        # elif refresh is None:
        #     stream_harvest.harvest_options.refresh = True
        stream_harvest.harvest_options.refresh = False
    else:
        stream_harvest.harvest_options.refresh = True
    logger.info(f"Refresh flag: {stream_harvest.harvest_options.refresh}")
    return stream_harvest

@task#(log_stdout=True)
def check_requested(stream_harvest):
    logger = get_run_logger()
    status_json = stream_harvest.status.dict()
    if status_json.get("status") == 'discontinued':
        # Skip discontinued stuff forever
        # TODO: Find way to turn off the scheduled flow all together
        #raise SKIP("Stream is discontinued. Finished.")
        return Completed(message="Stream is discontinued. Finished.")

    if stream_harvest.harvest_options.refresh is True:
        return stream_harvest.status.data_check

    last_data_date = parser.parse(status_json.get("end_date") + 'Z')
    logger.info(f"Cloud -- Last data point: {last_data_date}")

    if stream_harvest.status.data_check is True:
        return True
    elif (
        status_json.get("status") == 'success'
        and status_json.get("data_ready") is True
    ):
        # Get end time from OOI system
        current_end_dt = _check_stream(stream_harvest)
        logger.info(f"OOI -- Last data point: {current_end_dt}")
        data_diff = current_end_dt - last_data_date
        if status_json.get("process_status") == 'success':
            logger.info(f"Current data difference: {data_diff}")
            if data_diff > datetime.timedelta(minutes=1):
                # The last time is ready, but now it's been an hour so
                # request new data
                return False
            else:
                #raise SKIP("Skipping harvest. No new data needed.")
                return Completed(message="Skipping harvest. No new data needed.")
        # data is ready for processing!
        return True
    # else:
    #     return False

def _check_stream(stream_harvest):
    import json
    from requests_html import HTMLSession
    from data_vent.settings.main import harvest_settings

    session = HTMLSession()

    site, subsite, port, inst = stream_harvest.instrument.split('-')
    resp = session.get(
        f"https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/{site}/{subsite}/{port}-{inst}/metadata/times",
        auth=(
            harvest_settings.ooi_config.username,
            harvest_settings.ooi_config.token,
        ),
    )
    if resp.status_code == 200:
        try:
            all_streams = resp.json()
            current_end_dt = next(
                filter(
                    lambda s: s['stream'] == stream_harvest.stream.name
                    and s['method'] == stream_harvest.stream.method,
                    all_streams,
                )
            )['endTime']
            return parser.parse(current_end_dt)
        except json.JSONDecodeError:
            # If it's not JSON, get the title of the page
            html_title = resp.html.find('title', first=True).text.lower()
            if 'maintenance' in html_title:
                # if there's maintainance then skip
                # raise SKIP("OOI is under maintenance!")
                return Completed(message="OOI is under maintenance!")
    #TODO not sure what this did
    #raise SKIP("OOINet is currently down.")

#TODO max retries is limited to prefect 1
@task(retries=6, retry_delay_seconds=600)
def setup_harvest(stream_harvest: StreamHarvest):
    logger = get_run_logger()
    logger.info("=== Setting up data request ===")
    table_name = stream_harvest.table_name
    streams_list = fetch_streams_list(stream_harvest)
    request_dt = datetime.datetime.utcnow().isoformat()
    status_json = stream_harvest.status.dict()
    try:
        stream_dct = next(
            filter(lambda s: s['table_name'] == table_name, streams_list)
        )
    except Exception:
        # Check if stream has been dicontinued
        logger.warning("Stream not found in OOI Database.")
        message = f"{table_name} not found in OOI Database. It may be that this stream has been discontinued."
        status_json.update(
            {'status': 'discontinued', 'last_refresh': request_dt}
        )
        update_and_write_status(stream_harvest, status_json)
        # raise SKIP(
        #     message=message, result={"status": status_json, "message": message})
        return Cancelled(message=message, result={"status": status_json, "message": message})
        
    if stream_harvest.harvest_options.goldcopy:
        message = "Gold Copy Harvest is not currently supported."
        logger.warning(message)
        status_json.update({'status': 'failed'})
        update_and_write_status(stream_harvest, status_json)
        # raise SKIP(message=message, result={"status": status_json, "message": message},)
        return Cancelled(message=message, result={"status": status_json, "message": message})

    else:
        estimated_request = create_request_estimate(
            stream_dct=stream_dct,
            start_dt=stream_harvest.harvest_options.custom_range.start,
            end_dt=stream_harvest.harvest_options.custom_range.end,
            refresh=stream_harvest.harvest_options.refresh,
            existing_data_path=stream_harvest.harvest_options.path,
            request_kwargs=dict(provenance=True),
            storage_options=stream_harvest.harvest_options.path_settings
        )

    estimated_request.setdefault("request_dt", request_dt)
    message = "Data Harvest has been setup successfully."
    logger.info(message)
    return estimated_request


# TODO: Create state handler that update to request.yaml
# TODO: Save request_response to response.json
@task
def request_data(
    estimated_request: Dict[str, Any],
    stream_harvest: StreamHarvest,
    force_harvest: bool = False
):
    logger = get_run_logger()
    status_json = stream_harvest.status.dict()
    logger.info("=== Performing data request ===")
    if "requestUUID" in estimated_request['estimated']:
        logger.info("Continue to actual request ...")
        request_response = perform_request(
            estimated_request,
            refresh=stream_harvest.harvest_options.refresh,
            logger=logger,
            storage_options=stream_harvest.harvest_options.path_settings,
            force=force_harvest
        )
        result = request_response.get('result', None)
        if result is None or 'status_code' in result:
            logger.info("Writing out data request status to failed ...")
            status_json.update(
                {
                    'status': 'failed',
                    'data_ready': False,
                    'data_response': request_response.get("file_path"),
                    'requested_at': datetime.datetime.utcnow() if result is None else result.get("request_dt"),
                    'data_check': False
                }
            )
            update_and_write_status(stream_harvest, status_json)
            if result is None:
                message="Error found with ooi-harvester during request"
            else:
                message=f"Error found with OOI M2M during request: ({result.get('status_code')}) {result.get('reason')}"
            # TODO here the prefect 1.0 flag is FAIL
            # raise FAIL(
            #     message=message,
            #     result={"status": status_json, "message": message},
            # )
            return Failed(message=message, result={"status": status_json, "message": message})
        else:
            status_json.update(
                {
                    'status': 'pending',
                    'data_ready': False,
                    'data_response': request_response.get("file_path"),
                    'requested_at': request_response['result']['request_dt'],
                    'data_check': True
                }
            )
            update_and_write_status(stream_harvest, status_json)
            return request_response
    else:
        logger.info("Writing out status to failed ...")
        status_json.update({'status': 'failed'})
        update_and_write_status(stream_harvest, status_json)
        message = "No data is available for harvesting."
        # raise SKIP(
        #     message="No data is available for harvesting.",
        #     result={"status": status_json, "message": message},
        # )
        # TODO another prefect 1.0 engine signal 
        return Cancelled(message="No data is available for harvesting.",
                         result={"status": status_json, "message": message})


@task
def get_request_response(stream_harvest: StreamHarvest, logger=None):
    if logger is None:
        logger = get_run_logger()
    stream_harvest = read_status_json(stream_harvest)
    try:
        # Read the data response json from s3 cache
        with fsspec.open(
            stream_harvest.status.data_response,
            **stream_harvest.harvest_options.path_settings,
        ) as f:
            request_response = json.load(f)
    except FileNotFoundError as e:
        # Data response file not found
        # may be due to auto deletion by S3
        logger.warning(f"Missing data response file: {stream_harvest.status.data_response}")
        status_json = stream_harvest.status.dict()

        # daily harvest
        if stream_harvest.status.data_response.endswith('daily'):
            status_json.update(
                {
                    'status': 'success',
                    'process_status': 'success',
                    'data_check': False,
                    'data_ready': True,
                }
            )
        # refresh harvest
        elif stream_harvest.status.data_response.endswith('refresh'):
            status_json.update(
                {
                    'status': 'unknown',
                    'process_status': None,
                    'data_check': False,
                    'last_refresh': None,
                    'data_ready': False,
                }
            )
        else:
            # if neither daily or refresh raise the exception
            raise e
        
        message = "Skipping for now and retrying again later due to missing data response file."
        update_and_write_status(stream_harvest, status_json)
        
        # TODO can you raise Cancelled function?
        raise Cancelled(
            message=message,
            result={"status": status_json, "message": message},
        )
        
    return request_response


# TODO: Create state handler that update to request.yaml each time check_data is run
@task(retries=6, retry_delay_seconds=600)
def check_data(data_response, stream_harvest):
    logger = get_run_logger()
    logger.info("=== Checking for data readiness ===")
    status_json = stream_harvest.status.dict()
    result = data_response.get("result")
    status_url = result.get("status_url", None)
    if status_url is not None:
        in_progress = check_in_progress(status_url)
        if not in_progress:
            logger.info("Data available for download.")
            status_json.update(
                {'status': 'success', 'data_ready': True}
            )
            stream_harvest = update_and_write_status(
                stream_harvest, status_json
            )
            return {
                'data_response': data_response,
                'stream_harvest': stream_harvest,
            }
        else:
            time_since_request = (
                datetime.datetime.utcnow()
                - dateutil.parser.parse(data_response['result']['request_dt'])
            )
            if time_since_request >= datetime.timedelta(days=2):
                try:
                    catalog_dict = parse_response_thredds(data_response)
                    filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)
                    if len(filtered_catalog_dict['datasets']) > 0:
                        logger.info(
                            "Data request timeout reached. But nc files are still available."
                        )
                        status_json.update(
                            {
                                'status': 'success',
                                'data_ready': True,
                            }
                        )
                        stream_harvest = update_and_write_status(
                            stream_harvest, status_json
                        )
                        return {
                            'data_response': data_response,
                            'stream_harvest': stream_harvest,
                        }
                except Exception:
                    message = f"Data request timeout reached. Has been waiting for more than 2 days. ({str(time_since_request)}) | {status_url}"
                    status_json.update(
                        {
                            'status': 'failed',
                            'data_ready': False,
                        }
                    )
                    update_and_write_status(stream_harvest, status_json)
                    #TODO can I raise Cancelled?
                    raise Cancelled(
                        message=message,
                        result={"status": status_json, "message": message},
                    )
            else:
                logger.info(
                    f"Data request time elapsed: {str(time_since_request)}"
                )
                message = "Data is not ready for download..."
                status_json.update(
                    {
                        'status': 'pending',
                        'data_ready': False,
                        'data_check': True,
                    }
                )
                update_and_write_status(stream_harvest, status_json)
                #TODO can I raise Cancelled?
                raise Cancelled(
                    message=message,
                    result={"status": status_json, "message": message},
                )
