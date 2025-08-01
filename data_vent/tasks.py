import datetime
from typing import Any, Dict
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

from prefect import task, get_run_logger
from prefect.states import Cancelled, Failed

from data_vent.producer import (
    StreamHarvest,
    fetch_streams_list,
    create_request_estimate,
    perform_request,
)
from data_vent.processor import (
    _download,
    _update_time_coverage,
    update_metadata,
    chunk_ds,
    append_to_zarr,
    is_zarr_ready,
    preproc,
)
from data_vent.processor.checker import check_in_progress
from data_vent.processor.utils import _write_data_avail, _get_var_encoding
from data_vent.processor.pipeline import _fetch_avail_dict

from data_vent.utils.parser import (
    parse_exception,
    parse_response_thredds,
    filter_and_parse_datasets,
    setup_etl,
)
from data_vent.utils.validate import (
    check_for_timestamp_duplicates, 
    check_for_empty_qartod_vars
)

from data_vent.settings import harvest_settings
from data_vent.config import FLOW_PROCESS_BUCKET
from data_vent.config import STORAGE_OPTIONS
from data_vent.exceptions import DataNotReadyError, NullMetadataError, StreamNotFoundError


def setup_status_s3fs(
    stream_harvest: StreamHarvest,
):
    fs = fsspec.filesystem(
        "s3",
        **STORAGE_OPTIONS["aws"],  # **stream_harvest.harvest_options.path_settings
    )
    status_file = f"{FLOW_PROCESS_BUCKET}/harvest-status/{stream_harvest.table_name}"

    return fs, status_file


def write_status_json(
    stream_harvest: StreamHarvest,
):
    fs, status_file = setup_status_s3fs(stream_harvest)
    status_json = stream_harvest.status.model_dump()
    # write status file from s3 if exists
    with fs.open(status_file, mode="w") as f:
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
    stream_harvest: StreamHarvest, status_json: Dict[str, Any], write: bool = True
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
    config_json: Dict[str, Any],
    harvest_options: Dict[str, Any] = {},
    force_harvest: bool = False,
    refresh: bool = False,
):
    logger = get_run_logger()
    config_json["harvest_options"].update(harvest_options)
    stream_harvest = StreamHarvest(**config_json)
    stream_harvest = read_status_json(stream_harvest)

    # if stream_harvest.status.last_refresh is not None:
    logger.info(f"Cloud data last refreshed on {stream_harvest.status.last_refresh}")
    # 11/28/2022 Don.S: Comment out this section to prevent auto refresh.
    # refresh = harvest_options.get('refresh', None)
    # last_refresh = parser.parse(stream_harvest.status.last_refresh)
    # current_dt = datetime.datetime.utcnow()
    # if (current_dt - last_refresh) < datetime.timedelta(days=30):
    #     if refresh is None:
    #         stream_harvest.harvest_options.refresh = False
    # elif refresh is None:
    #     stream_harvest.harvest_options.refresh = True
    #     stream_harvest.harvest_options.refresh = False
    if refresh:
        stream_harvest.harvest_options.refresh = True
    if force_harvest:
        stream_harvest.harvest_options.force_harvest = True
    # TODO Don clearly intended to pass arguments around in tidy pydantic jsons
    # but right now we have the harvest behavior we want using a messing combination
    # of piping arguments and passing around those models. This should be cleaned up
    # but for now its stable...
    logger.warning(f"Refresh flag: {stream_harvest.harvest_options.refresh}")
    logger.warning(f"Force harvest: {stream_harvest.harvest_options.force_harvest}")
    return stream_harvest


@task
def check_requested(stream_harvest):
    logger = get_run_logger()
    status_json = stream_harvest.status.model_dump()
    if status_json.get("status") == "discontinued":
        # Raise an error if the harvest has stream registered as discontinued
        raise StreamNotFoundError("Stream is discontinued. Cannot proceed with harvest.")

     
    if stream_harvest.harvest_options.refresh is True:
        return stream_harvest.status.data_check

    try:
        last_data_date = parser.parse(status_json.get("end_date") + "Z")
    except TypeError as e:
        raise NullMetadataError(
            f"Unable to parse end date in status json: {str(e)}: This is "
            "likely because this stream was recently run with flag `refresh` and `force-harvest`. "
            "Try running the stream again with `refresh` set to `True` and `force-harvest` set "
            "to `False`. If the full time series is ready, it will be processed to zarr and "
            "valid start_dates and end_dates added to the json. Once complete APPENDS can be "
            "run again succesfully."
        )
    logger.info(f"RCA Cloud -- Last data point: {last_data_date}")

    if stream_harvest.status.data_check is True:
        return True
    elif status_json.get("status") == "success" and status_json.get("data_ready") is True:
        # Get end time from OOI system
        current_end_dt = _check_stream(stream_harvest)
        logger.info(f"OOI -- Last data point: {current_end_dt}")
        data_diff = current_end_dt - last_data_date
        if status_json.get("process_status") == "success":
            logger.info(f"Current data difference: {data_diff}")
            if data_diff > datetime.timedelta(minutes=1):
                # The last time is ready, but now it's been an hour so
                # request new data
                return False
            else:
                return "SKIPPED"  # Skipping harvest. No new data needed.
        # data is ready for processing!
        return True
    else:
        return False


@task
def reset_status_json(stream_harvest):
    status_json = stream_harvest.status.model_dump()
    # setting end date to None is #HACK to get subsequent refresh streams to fail nicely
    # could make a nice error #TODO in future
    status_json.update({"data_check": False, "start_date": None, "end_date": None})
    update_and_write_status(stream_harvest, status_json)


def _check_stream(stream_harvest):
    import json
    from requests_html import HTMLSession
    from data_vent.settings.main import harvest_settings

    session = HTMLSession()
    logger = get_run_logger()

    site, subsite, port, inst = stream_harvest.instrument.split("-")
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
            logger.info(f"all streams: {all_streams}")
            current_end_dt = next(
                filter(
                    lambda s: s["stream"] == stream_harvest.stream.name
                    and s["method"] == stream_harvest.stream.method,
                    all_streams,
                )
            )["endTime"]
            logger.info(f"current_end_dt: {current_end_dt}")
            return parser.parse(current_end_dt)
        except json.JSONDecodeError:
            # If it's not JSON, get the title of the page
            html_title = resp.html.find("title", first=True).text.lower()
            if "maintenance" in html_title:
                # if there's maintainance then skip
                # raise SKIP("OOI is under maintenance!")
                logger.warning("OOI is under maintenance!")
                return Cancelled(message="OOI is under maintenance!")
    # raise SKIP("OOINet is currently down.")


@task(retries=6, retry_delay_seconds=600)
def setup_harvest(stream_harvest: StreamHarvest):
    logger = get_run_logger()
    logger.info("=== Setting up data request ===")
    table_name = stream_harvest.table_name
    streams_list = fetch_streams_list(stream_harvest)
    request_dt = datetime.datetime.utcnow().isoformat()
    status_json = stream_harvest.status.model_dump()
    try:
        stream_dct = next(filter(lambda s: s["table_name"] == table_name, streams_list))
    except StopIteration:
        # since we are just harvesting RCA we don't want these to fail quietly anymore
        message = f"{table_name} not found in OOI Database. It may be that this stream has been discontinued."
        status_json.update({"status": "failed", "last_refresh": request_dt})
        update_and_write_status(stream_harvest, status_json)

        raise StreamNotFoundError(message)

    if stream_harvest.harvest_options.goldcopy:
        message = "Gold Copy Harvest is not currently supported."
        logger.warning(message)
        status_json.update({"status": "failed"})
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
            storage_options=stream_harvest.harvest_options.path_settings,
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
    force_harvest: bool = False,
):
    logger = get_run_logger()
    status_json = stream_harvest.status.model_dump()
    logger.info("=== Performing data request ===")
    if "requestUUID" in estimated_request["estimated"]:
        logger.info("Continue to actual request ...")
        logger.info(estimated_request)
        request_response = perform_request(
            estimated_request,
            refresh=stream_harvest.harvest_options.refresh,
            logger=logger,
            storage_options=stream_harvest.harvest_options.path_settings,
            force=force_harvest,
        )
        result = request_response.get("result", None)
        if result is None or "status_code" in result:
            logger.info("Writing out data request status to failed ...")
            status_json.update(
                {
                    "status": "failed",
                    "data_ready": False,
                    "data_response": request_response.get("file_path"),
                    "requested_at": datetime.datetime.utcnow()
                    if result is None
                    else result.get("request_dt"),
                    "data_check": False,
                }
            )
            update_and_write_status(stream_harvest, status_json)
            if result is None:
                message = "Error found with ooi-harvester during request"
            else:
                message = f"Error found with OOI M2M during request: ({result.get('status_code')}) {result.get('reason')}"
            # raise FAIL(
            #     message=message,
            #     result={"status": status_json, "message": message},
            # )
            logger.warning(message)
            return Failed(message=message, result={"status": status_json, "message": message})
        else:
            status_json.update(
                {
                    "status": "pending",
                    "data_ready": False,
                    "data_response": request_response.get("file_path"),
                    "requested_at": request_response["result"]["request_dt"],
                    "data_check": True,
                }
            )
            update_and_write_status(stream_harvest, status_json)
            return request_response
    else:
        logger.info("Writing out status to failed ...")
        status_json.update({"status": "failed"})
        update_and_write_status(stream_harvest, status_json)
        message = "No data is available for harvesting."
        # raise SKIP(
        #     message="No data is available for harvesting.",
        #     result={"status": status_json, "message": message},
        # )
        logger.warning(message)
        return Cancelled(
            message="No data is available for harvesting.",
            result={"status": status_json, "message": message},
        )


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
        status_json = stream_harvest.status.model_dump()

        # daily harvest
        if stream_harvest.status.data_response.endswith("daily"):
            status_json.update(
                {
                    "status": "success",
                    "process_status": "success",
                    "data_check": False,
                    "data_ready": True,
                }
            )
        # refresh harvest
        elif stream_harvest.status.data_response.endswith("refresh"):
            status_json.update(
                {
                    "status": "unknown",
                    "process_status": None,
                    "data_check": False,
                    "last_refresh": None,
                    "data_ready": False,
                }
            )
        else:
            # if neither daily or refresh raise the exception
            raise e

        message = (
            "Skipping for now and retrying again later due to missing data response file."
        )
        update_and_write_status(stream_harvest, status_json)

        logger.warning(message)
        return Cancelled(
            message=message,
            result={"status": status_json, "message": message},
        )

    return request_response


# TODO: Create state handler that update to request.yaml each time check_data is run
@task(retries=6, retry_delay_seconds=300)
def check_data(data_response, stream_harvest):
    logger = get_run_logger()
    logger.info("=== Checking for data readiness ===")
    status_json = stream_harvest.status.model_dump()
    result = data_response.get("result")
    status_url = result.get("status_url", None)
    if status_url is not None:
        in_progress = check_in_progress(status_url)
        if not in_progress:
            logger.info("Data available for download.")
            status_json.update({"status": "success", "data_ready": True})
            stream_harvest = update_and_write_status(stream_harvest, status_json)
            return {
                "data_response": data_response,
                "stream_harvest": stream_harvest,
            }
        else:
            time_since_request = datetime.datetime.utcnow() - dateutil.parser.parse(
                data_response["result"]["request_dt"]
            )
            if time_since_request >= datetime.timedelta(days=2):
                try:
                    catalog_dict = parse_response_thredds(data_response)
                    filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)
                    if len(filtered_catalog_dict["datasets"]) > 0:
                        logger.info(
                            "Data request timeout reached. But nc files are still available."
                        )
                        status_json.update(
                            {
                                "status": "success",
                                "data_ready": True,
                            }
                        )
                        stream_harvest = update_and_write_status(stream_harvest, status_json)
                        return {
                            "data_response": data_response,
                            "stream_harvest": stream_harvest,
                        }
                except Exception:
                    message = f"Data request timeout reached. Has been waiting for more than 2 days. ({str(time_since_request)}) | {status_url}"
                    status_json.update(
                        {
                            "status": "failed",
                            "data_ready": False,
                        }
                    )
                    update_and_write_status(stream_harvest, status_json)
                    logger.warning(message)
                    return Cancelled(
                        message=message,
                        result={"status": status_json, "message": message},
                    )
            else:
                logger.info(f"Data request time elapsed: {str(time_since_request)}")
                message = "Data is not ready for download..."
                status_json.update(
                    {
                        "status": "pending",
                        "data_ready": False,
                        "data_check": True,
                    }
                )
                update_and_write_status(stream_harvest, status_json)

                raise DataNotReadyError


@task
def get_response(data_response):
    return data_response.get("data_response")


@task
def get_stream(data_response):
    return data_response.get("stream_harvest")


@task
def setup_process(response_json, target_bucket):
    logger = get_run_logger()
    logger.info("=== Setting up process ===")
    catalog_dict = parse_response_thredds(response_json)
    filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)
    harvest_catalog = dict(**filtered_catalog_dict, **response_json)
    nc_files_dict = setup_etl(harvest_catalog, target_bucket=target_bucket)
    logger.info(f"{len(nc_files_dict.get('datasets', []))} netcdf files to be processed.")
    return nc_files_dict


@task
def data_processing(
    nc_files_dict, 
    stream_harvest, 
    max_chunk, 
    refresh, 
    overwrite_attrs,
    check_qartod,
):
    logger = get_run_logger()
    stream = nc_files_dict.get("stream")
    name = stream.get("table_name")
    logger.info(f"=== Processing {name}. ===")
    status_json = stream_harvest.status.model_dump()
    status_json.update({"process_status": "pending"})
    update_and_write_status(stream_harvest, status_json)
    dataset_list = sorted(nc_files_dict.get("datasets", []), key=lambda i: i.get("start_ts"))
    temp_zarr = nc_files_dict.get("temp_bucket")
    temp_store = fsspec.get_mapper(
        temp_zarr,
        **stream_harvest.harvest_options.path_settings,
    )

    existing_enc = None
    if not stream_harvest.harvest_options.refresh:
        final_zarr = nc_files_dict.get("final_bucket")
        final_store = fsspec.get_mapper(
            final_zarr,
            **stream_harvest.harvest_options.path_settings,
        )
        zg = zarr.open_consolidated(final_store)
        existing_enc = {k: _get_var_encoding(var) for k, var in zg.arrays()}
        # change "temp" to the actual final when daily append
        temp_zarr = final_zarr
        temp_store = final_store

    if len(dataset_list) > 0:
        for idx, d in enumerate(dataset_list):
            is_first = False
            if idx == 0:
                if stream_harvest.harvest_options.refresh:
                    # Append to live data when it's daily
                    # So it's never the first
                    is_first = True
              
            logger.info(
                f"*** {name} ({d.get('deployment')}) | {d.get('start_ts')} - {d.get('end_ts')} ***"
            )
            with tempfile.TemporaryDirectory() as tmpdir:
                source_url = "/".join([nc_files_dict.get("async_url"), d.get("name")])
                # Download the netcdf files and read to a xarray dataset obj
                ncpath = _download(
                    source_url=source_url,
                    cache_location=tmpdir,
                )
                logger.info(f"Downloaded: {ncpath}")
                with dask.config.set(scheduler="single-threaded"):
                    ds = (
                        xr.open_dataset(ncpath, engine="netcdf4", decode_times=False)
                        .pipe(preproc)
                        .pipe(
                            update_metadata,
                            nc_files_dict.get("retrieved_dt"),
                        )
                    )
                    # <<< SOME DATA VALIDATION depending on context >>>
                    # only check for duplicate timestamps during daily appends
                    if not refresh:
                        check_for_timestamp_duplicates(ds)
                    if refresh and check_qartod:
                        ds = check_for_empty_qartod_vars(ds)

                    logger.info("Finished preprocessing dataset.")

                    # Chunk dataset and write to zarr
                    if isinstance(ds, xr.Dataset):
                        mod_ds, enc = chunk_ds(
                            ds,
                            max_chunk=max_chunk,
                            existing_enc=existing_enc,
                            apply=is_first,
                        )
                        logger.info("Finished chunking dataset.")

                        if is_first:
                            # TODO: Like the _prepare_ds_to_append need to check on the dims and len for all variables
                            mod_ds.to_zarr(
                                temp_store,
                                consolidated=True,
                                compute=True,
                                mode="w",
                                encoding=enc,
                            )
                            succeed = True
                        else:
                            succeed = append_to_zarr(
                                mod_ds, temp_store, enc, overwrite_attrs, logger=logger
                            )

                        if succeed:
                            is_done = False
                            while not is_done:
                                store = fsspec.get_mapper(
                                    temp_zarr,
                                    **stream_harvest.harvest_options.path_settings,
                                )
                                is_done = is_zarr_ready(store)
                                if is_done:
                                    continue
                                time.sleep(5)
                                logger.info("Waiting for zarr file writing to finish...")
                            logger.info("SUCCESS: File successfully written to zarr.")
                        else:
                            logger.warning(
                                f"SKIPPED: Issues in file found for {d.get('name')}!"
                            )
                    else:
                        logger.warning("SKIPPED: Failed pre processing!")

    else:
        # raise SKIP("No datasets to process. Skipping...")
        logger.warning("No datasets to process. Skipping...")
        return Cancelled(message="No datasets to process. Skipping...")
    return {
        "final_path": nc_files_dict.get("final_bucket"),
        "temp_path": nc_files_dict.get("temp_bucket"),
    }


@task
def finalize_data_stream(stores_dict, stream_harvest, max_chunk):
    logger = get_run_logger()
    logger.info("=== Finalizing data stream. ===")
    try:
        final_path = stores_dict.get("final_path")
        status_json = stream_harvest.status.model_dump()
        final_store = fsspec.get_mapper(
            final_path,
            **stream_harvest.harvest_options.path_settings,
        )
        temp_store = fsspec.get_mapper(
            stores_dict.get("temp_path"),
            **stream_harvest.harvest_options.path_settings,
        )
        if stream_harvest.harvest_options.refresh:
            # Remove missing groups in the final store
            temp_group = zarr.open_consolidated(temp_store)
            final_group = zarr.open_group(final_store, mode="a")
            final_modified = False
            for k, _ in final_group.items():
                if k not in list(temp_group.array_keys()):
                    final_group.pop(k)
                    final_modified = True

            if final_modified:
                zarr.consolidate_metadata(final_store)

            # Copy over the store, at this point, they should be similar
            zarr.copy_store(temp_store, final_store, if_exists="replace")
        # NOTE: Comment out since append to live data happened during
        # data_processing task
        # else:
        #     zg = zarr.open_consolidated(final_store)
        #     existing_enc = {k: _get_var_encoding(var) for k, var in zg.arrays()}
        #     temp_ds = xr.open_dataset(
        #         temp_store,
        #         engine='zarr',
        #         backend_kwargs={'consolidated': True},
        #         decode_times=False,
        #     )
        #     mod_ds, enc = chunk_ds(temp_ds, max_chunk=max_chunk, apply=False, existing_enc=existing_enc)
        #     succeed = append_to_zarr(mod_ds, final_store, enc, logger=logger)
        #     if succeed:
        #         is_done = False
        #         while not is_done:
        #             store = fsspec.get_mapper(
        #                 final_path,
        #                 **stream_harvest.harvest_options.path_settings,
        #             )
        #             is_done = is_zarr_ready(store)
        #             if is_done:
        #                 continue
        #             time.sleep(5)
        #             logger.info("Waiting for zarr file writing to finish...")
        #     else:
        #         status_json.update(
        #             {
        #                 'process_status': 'failed',
        #                 'cloud_location': final_path,
        #                 'processed_at': datetime.datetime.utcnow().isoformat(),
        #             }
        #         )
        #         update_and_write_status(stream_harvest, status_json)
        #         raise FAIL(f"Issues in file found for {final_path}!")

        # Update start and end date in global attributes
        start_dt, end_dt = _update_time_coverage(final_store)

        if stream_harvest.harvest_options.refresh:
            # Clean up temp_store
            # no temp store was created during daily append
            temp_store.clear()
        logger.info(f"Data stream finalized: {final_path}")
        status_json.update(
            {
                "process_status": "success",
                "cloud_location": final_path,
                "start_date": start_dt,
                "end_date": end_dt,
                "processed_at": datetime.datetime.utcnow().isoformat(),
                "data_check": False,
            }
        )
        if stream_harvest.harvest_options.refresh is True:
            status_json.update(
                {
                    "last_refresh": datetime.datetime.utcnow().isoformat(),
                }
            )
        update_and_write_status(stream_harvest, status_json)
        return final_path
    except Exception as e:
        status_json.update(
            {
                "process_status": "failed",
                "processed_at": datetime.datetime.utcnow().isoformat(),
            }
        )
        update_and_write_status(stream_harvest, status_json)
        exc_dict = parse_exception(e)
        raise Failed(message=exc_dict.get("traceback", str(e)), result=exc_dict)


@task
def data_availability(nc_files_dict, stream_harvest, export=False, gh_write=False):
    name = nc_files_dict["stream"]["table_name"]
    inst_rd = nc_files_dict["stream"]["reference_designator"]
    stream_rd = "-".join(
        [nc_files_dict["stream"]["method"], nc_files_dict["stream"]["stream"]]
    )
    logger = get_run_logger()
    logger.info(f"availability for {name}.")

    url = nc_files_dict["final_bucket"]
    mapper = fsspec.get_mapper(url, **stream_harvest.harvest_options.path_settings)
    try:
        za = zarr.open_consolidated(mapper)["time"]
        calendar = za.attrs.get("calendar", harvest_settings.ooi_config.time["calendar"])
        units = za.attrs.get("units", harvest_settings.ooi_config.time["units"])

        if any(np.isnan(za)):
            logger.info(f"Null values found. Skipping {name}")
        else:
            logger.info(f"Total time bytes: {dask.utils.memory_repr(za.nbytes)}")
            darr = da.from_zarr(za)

            darr_dt = darr.map_blocks(
                xr.coding.times.decode_cf_datetime,
                units=units,
                calendar=calendar,
            )

            ddf = darr_dt.to_dask_dataframe(["dtindex"]).set_index("dtindex")
            ddf["count"] = 0

            resolutions = {"hourly": "H", "daily": "D", "monthly": "M"}
            result_dict = {}
            for k, v in resolutions.items():
                try:
                    result = _fetch_avail_dict(ddf, resolution=v)
                    result_dict.update({k: result})
                except Exception as e:
                    if k == "daily":
                        raise e
                    logger.warning(f"ERROR: Creation of {k} data availability :: {e}")

            avail_dict = {
                "data_stream": stream_rd,
                "inst_rd": inst_rd,
                "results": result_dict,
            }
            if export:
                _write_data_avail(avail_dict, gh_write=gh_write)

            return avail_dict
    except Exception as e:
        exc_dict = parse_exception(e)
        raise Failed(message=exc_dict.get("traceback", str(e)), result=exc_dict)
