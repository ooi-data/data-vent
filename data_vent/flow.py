from typing import Any, Dict, Optional, List

import os
import yaml
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from prefect.states import Completed

import fsspec

from data_vent.tasks import (
    get_stream_harvest,
    check_requested,
    reset_status_json,
    setup_harvest,
    request_data,
    get_request_response,
    check_data,
    get_response,
    get_stream,
    setup_process,
    data_processing,
    finalize_data_stream,
    data_availability,
    read_status_json,
)

from data_vent.config import STORAGE_OPTIONS, DATA_BUCKET, COMPUTE_EXCEPTIONS


class FlowParameters(BaseModel):
    config: Optional[Dict[str, Any]] = None
    target_bucket: str = f"s3://{DATA_BUCKET}"
    force_harvest: bool = False
    refresh: bool = False
    max_chunk: str = "100MB"
    export_da: bool = False
    gh_write_da: bool = False
    overwrite_attrs: Optional[bool] = False
    check_qartod: Optional[bool] = False


@flow
def stream_ingest(
    config: Dict,
    harvest_options: Dict[str, Any] = {},
    force_harvest: bool = False,
    refresh: bool = False,
    max_chunk: str = "100MB",
    target_bucket: str = f"s3://{DATA_BUCKET}",
    export_da: bool = True,
    gh_write_da: bool = True,
    overwrite_attrs: Optional[bool] = False,
    check_qartod: Optional[bool] = False,
):
    logger = get_run_logger()

    # Check default_params
    flow_params = FlowParameters(
        config=config,
        target_bucket=target_bucket,
        force_harvest=force_harvest,
        refresh=refresh,
        max_chunk=max_chunk,
        export_da=export_da,
        gh_write_da=gh_write_da,
        overwrite_attrs=overwrite_attrs,
        check_qartod=check_qartod,
    )

    flow_dict = flow_params.model_dump()

    stream_harvest = get_stream_harvest(
        flow_dict.get("config"), harvest_options, force_harvest, refresh
    )

    stream_harvest.harvest_options.path_settings = STORAGE_OPTIONS["aws"]

    # when refreshing we need to reset the `data_check`` flag to False to ensure a fresh data
    # request to m2m, because we don't know what state that flag was left in due to the failure
    # that is likely necessitating the refresh...
    if refresh and force_harvest:
        logger.info("force havest - resetting data check flag")
        reset_status_json(stream_harvest)

    is_requested = check_requested(stream_harvest)

    while not is_requested:
        # Run the data request here
        estimated_request = setup_harvest(stream_harvest)

        request_response = request_data(estimated_request, stream_harvest, force_harvest)

        logger.info("Confirming data has been succesfully requested")
        read_status_json(stream_harvest)

        is_requested = check_requested(stream_harvest)

    if is_requested == "SKIPPED":
        return Completed(message="Skipping harvest. No new data needed.")

    logger.info("Data succesfully requested - checking data readiness")
    # Get request response directly here
    request_response = get_request_response(stream_harvest)
    # Now run the check - may need to adjust retries if different "waiting" behavior is needed
    data_readiness = check_data(request_response, stream_harvest)

    response_json = get_response(data_readiness)
    stream_harvest = get_stream(data_readiness)

    # Process data to temp
    nc_files_dict = setup_process(response_json, target_bucket)
    stores_dict = data_processing(
        nc_files_dict,
        stream_harvest,
        max_chunk,
        refresh,
        overwrite_attrs,
        check_qartod,
    )

    # Finalize data and transfer to final
    final_path = finalize_data_stream(
        stores_dict,
        stream_harvest,
        max_chunk,
    )

    # NOTE Data validation occurs where appropriate see utils.validate module for details
    # TODO data availability might be sunset completely, it is slow and not used downstream
    # at this point - could also remove gh_write_da and export_da args in future?
    # data_availability(
    #     nc_files_dict,
    #     stream_harvest,
    #     export_da,
    #     gh_write_da,
    #     wait_for=final_path,
    # )


@flow
def run_stream_ingest(
    streams: Optional[List[str]] = None,
    run_in_cloud: bool = True,
    # pipeline behavior args
    force_harvest: Optional[bool] = False,
    refresh: Optional[bool] = False,
    # mostly legacy CAVA args
    export_da: Optional[bool] = True,
    gh_write_da: Optional[bool] = True,
    # data validation args
    overwrite_attrs: Optional[bool] = False,
    check_qartod: Optional[bool] = False
):
    """
    Launches a data harvest for each specified OOI-RCA instrument streams

    In production setting harvesters run in parallel on AWS Fargate instances.

    Args:
        run_in_cloud (bool): If true, harvesters run in parallel on AWS Fargate instances orchestrated
            by a prefect deployment. Set to false to run harvesters in series on local machine. This
            can be useful for debugging.
        force_harvest (Optional[bool]): if `True` force pipeline to make harvest request of m2m
        refresh (Optional[bool]): whether to refresh stream data from t0, previously default to `False` but
            was set to `True` once per month
        export_da (Optional[bool]): arg from legacy pipeline, controls the uploading of data availability.
            Relevant to CAVA frontend/API.
        gh_write_da (Optional[bool]): arg from legacy pipeline, controls the uploading of data availability
            to gihub. Relevant to CAVA frontend/API.
        overwrite_attrs (Optional[bool]): if `True` overwrite both global and variable attributes in the 
            existing zarr with those in the most recent data.
        check_qartod (Optional[bool]): if `True` check for empty qartod data points and remove 
            them from the dataset so they don't cause havoc downstream. Only on refresh.

    As configured, harvesters will output array data stored as .zarr files to the following s3 buckets:

    flow-process-bucket: stores json-like harvest status to inform harvest logic
    temp-ooi-data-prod: temporary array storage for processing steps?
    ooi-data: final storage for processed array data - saved as .zarr

    """
    logger = get_run_logger()
    logger.info("Starting parent flow...")

    stage1_df = pd.read_csv(
        "https://raw.githubusercontent.com/OOI-CabledArray/rca-data-tools/main/rca_data_tools/qaqc/params/sitesDictionary.csv"  # noqa
    )
    stage2_df = pd.read_csv(
        "https://raw.githubusercontent.com/OOI-CabledArray/rca-data-tools/main/rca_data_tools/qaqc/params/stage2Dictionary.csv"
    )

    priority_df = pd.concat([stage1_df, stage2_df])

    # there may be some instruments we want to plot but not harvest
    priority_df = priority_df[priority_df["harvestInterval"] == 1]
    priority_instruments = sorted(priority_df.refDes.unique())

    config_dir = os.path.join(os.getcwd(), "flow_configs")

    if streams:
        all_paths = []
        for stream in streams:
            fpath = os.path.join(config_dir, stream[:27], f"{stream}.yaml")
            all_paths.append(fpath)

        logger.info(f"Running specified streams at the following paths: {all_paths}")

    else:  # grabs all config yamls
        fs = fsspec.filesystem("")
        glob_path = config_dir + "/**/*.yaml"
        logger.info(f"Searching for config yamls at path: {glob_path}")
        all_paths = fs.glob(glob_path)

        # instrument ref_des is first 27 characters of stream
        all_paths = [
            f for f in all_paths if os.path.basename(f)[:27] in priority_instruments
        ]

    for config_path in all_paths:
        config_json = yaml.safe_load(Path(config_path).open())
        run_name = "-".join(
            [
                config_json["instrument"],
                config_json["stream"]["method"],
                config_json["stream"]["name"],
            ]
        )

        flow_params = {
            "config": config_json,
            "target_bucket": f"s3://{DATA_BUCKET}",
            "force_harvest": force_harvest,
            "refresh": refresh,
            "max_chunk": "100MB",
            "export_da": export_da,
            "gh_write_da": gh_write_da,
            "overwrite_attrs": overwrite_attrs,
            "check_qartod": check_qartod,
        }

        logger.info(f"Launching child flow: {run_name}")
        logger.info(f"flow_params: {flow_params}")
        # run stream_ingest in parallel using AWS ECS fargate - this infrastructure is tied to
        # the prefect deployment
        if run_in_cloud:
            if refresh:
                harvest_type = "refresh"
            else:
                harvest_type = "append"

            if run_name in COMPUTE_EXCEPTIONS and harvest_type in COMPUTE_EXCEPTIONS[run_name]:
                deployment_name = (
                    f"stream-ingest/stream_ingest_{COMPUTE_EXCEPTIONS[run_name][harvest_type]}"
                )
                logger.warning(
                    f"{run_name} requires additional compute. Running deployment: {deployment_name}"
                )
            else:
                deployment_name = (
                    "stream-ingest/stream_ingest_2vcpu_16gb"  # default deployment
                )

            run_deployment(
                name=deployment_name,
                parameters=flow_params,
                flow_run_name=run_name,
                timeout=20,  # timeout can be adjusted based on AWS cluster race conditions
            )

        # run stream_ingest in sequence on local
        else:
            stream_ingest(**flow_params)

    logger.info("Parent flow complete")


if __name__ == "__main__":
    run_stream_ingest()
