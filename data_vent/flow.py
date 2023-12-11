from typing import Any, Dict, Optional, List

import asyncio
import os
import yaml
import pandas as pd
from pathlib import Path
from pydantic import BaseModel
from prefect import flow, get_run_logger, get_client
from prefect.deployments import run_deployment
from prefect.states import Completed

import fsspec
from data_vent.producer.models import StreamHarvest

from data_vent.tasks import (
    get_stream_harvest,
    check_requested,
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

from data_vent.settings.main import harvest_settings
from data_vent.pipelines.notifications import github_issue_notifier
from data_vent.config import STORAGE_OPTIONS, DATA_BUCKET


class FlowParameters(BaseModel):
    config: Optional[Dict[str, Any]]
    target_bucket: str = f"s3://{DATA_BUCKET}"
    force_harvest: bool = False
    refresh: bool = False
    max_chunk: str = "100MB"
    export_da: bool = False
    gh_write_da: bool = False
    error_test: bool = False


@flow
# TODO these parameters should be equivilant to prefect 1.0 version
def stream_ingest(
    config: Dict,
    harvest_options: Dict[str, Any] = {},
    # issue_config: Dict[str, Any] = {},
    force_harvest: bool = False,
    refresh: bool = False,
    max_chunk: str = "100MB",
    error_test: bool = False,
    target_bucket: str = f"s3://{DATA_BUCKET}",
    export_da: bool = False,  # TODO at least for testing
    gh_write_da: bool = False,  # TODO at least for testing
):
    logger = get_run_logger()

    # TODO automated github things - are these all deprecated??
    # default_gh_org = harvest_settings.github.data_org
    # issue_config.setdefault("gh_org", default_gh_org)
    # state_handlers = [github_issue_notifier(**issue_config)]

    # Check default_params
    flow_params = FlowParameters(
        config=config,
        target_bucket=target_bucket,
        force_harvest=force_harvest,
        refresh=refresh,
        max_chunk=max_chunk,
        export_da=export_da,
        gh_write_da=gh_write_da,
        error_test=error_test,
    )

    # Sets the defaults for flow config
    # config_required = False
    # if default_params.config is None:
    #     config_required = True

    flow_dict = flow_params.dict()

    # config = Parameter("config", required=config_required, default=default_dict.get("config", no_default),)
    # harvest_options = Parameter("harvest_options", default={})

    stream_harvest = get_stream_harvest(flow_dict.get("config"), harvest_options, refresh)
    # TODO how do you actually set these path settings - it is confusing
    # stream_harvest.harvest_options.path_settings = DEV_PATH_SETTINGS['aws']
    stream_harvest.harvest_options.path_settings = STORAGE_OPTIONS["aws"]

    is_requested = check_requested(
        stream_harvest
    )  # TODO return a string that determines fate of parent flow?

    while is_requested == False:
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
    # Now run the data check - #TODO may need to adjust retries based on new harvest logic
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
        error_test,
    )

    # Finalize data and transfer to final
    final_path = finalize_data_stream(
        stores_dict,
        stream_harvest,
        max_chunk,
        # task_args={
        #     "state_handlers": state_handlers,
        # },
    )

    # TODO: Add data validation step here!

    # Data availability
    availability = data_availability(
        nc_files_dict,
        stream_harvest,
        export_da,
        gh_write_da,
        # TODO figure out what to do with task args
        # task_args={
        #     "state_handlers": state_handlers,
        # },
        wait_for=final_path,  # TODO
    )

    # in prefect 1.0 this sets the provided task as an upstream dependency of `availability` in this case
    # availability.set_upstream(final_path)

    # TODO I think this saves logs as files to s3 - will have to find alternative or alter for prefect 2.0?
    # task_names = [t.name for t in stream_ingest.tasks]
    # if isinstance(log_settings, dict):
    #     log_settings = LogHandlerSettings(**log_settings)
    # elif isinstance(log_settings, LogHandlerSettings):
    #     ...
    # else:
    #     raise TypeError("log_settings must be type LogHandlerSettings or Dict")

    # flow_logger = get_logger()
    # flow_logger.addHandler(
    #     HarvestFlowLogHandler(task_names, **log_settings.dict())
    # )
    # return flow


# async def is_flowrun_running(run_name: str) -> bool:
#     async with get_client() as client:
#         flow_runs = await client.read_flow_runs()
#         for run in flow_runs: print(run.state)
#         running_runs = [run for run in flow_runs if run.state==Running()]


@flow
def run_stream_ingest(
    streams: Optional[List[str]] = None,
    test_run: bool = False,
    priority_only: bool = True,
    non_priority: bool = False,
    run_in_cloud: bool = True,
    # pipeline behavior args
    force_harvest: Optional[bool] = False,
    refresh: Optional[bool] = False,
    export_da: Optional[bool] = True,
    gh_write_da: Optional[bool] = True,
):
    """
    Launches a data harvest for each specified OOI-RCA instrument streams

    In production setting harvesters run in parallel on AWS Fargate instances.

    Args:
        test_run (bool): If true, only launch harvesters for 3 small test instrument streams
        priority_only (bool): If true, launch harvesters for instrument streams defined as
            priority in the OOI-RCA priority instrument csv
        non_priority (bool): if true, launch harvesters for all non-priority instruments
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

    As configured, harvesters will output array data stored as .zarr files to the following s3 buckets:

    flow-process-bucket: stores json-like harvest status to inform harvest logic
    temp-ooi-data-prod: temporary array storage for processing steps?
    ooi-data: final storage for processed array data - saved as .zarr

    """
    logger = get_run_logger()
    logger.info("Starting parent flow...")
    # validate arguments
    if priority_only and non_priority:
        raise ValueError(
            "non_priority must be `False` if priority only is `True` (or visa-versa)"
        )

    priority_df = pd.read_csv(
        "https://raw.githubusercontent.com/OOI-CabledArray/rca-data-tools/main/rca_data_tools/qaqc/params/sitesDictionary.csv"  # noqa
    )
    priority_instruments = sorted(priority_df.refDes.unique())

    config_dir = os.path.join(os.getcwd(), "flow_configs")

    if streams:
        all_paths = []
        for stream in streams:
            fpath = os.path.join(config_dir, stream[:27], f"{stream}.yaml")
            all_paths.append(fpath)

        logger.info(f"Running specified streams at the following paths: {all_paths}")

    elif test_run:
        # these are some "small data" streams that are useful for small test runs
        all_paths = [
            os.path.join(
                config_dir,
                "CE04OSPS-SF01B-2B-PHSENA108",
                "CE04OSPS-SF01B-2B-PHSENA108-streamed-phsen_data_record.yaml",
            ),
            os.path.join(
                config_dir,
                "CE04OSPS-SF01B-4F-PCO2WA102",
                "CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record.yaml",
            ),
            os.path.join(
                config_dir,
                "CE04OSPS-SF01B-4A-NUTNRA102",
                "CE04OSPS-SF01B-4A-NUTNRA102-streamed-nutnr_a_sample.yaml",
            ),
        ]
        logger.info(f"Using the following paths for this TEST RUN: {all_paths}")

    else:  # grabs all config yamls
        fs = fsspec.filesystem("")
        glob_path = config_dir + "/**/*.yaml"
        logger.info(f"Searching for config yamls at path: {glob_path}")
        all_paths = fs.glob(glob_path)

        if priority_only:
            # instrument ref_des is first 27 characters of stream
            all_paths = [
                f for f in all_paths if os.path.basename(f)[:27] in priority_instruments
            ]

        elif non_priority:
            all_paths = [
                f for f in all_paths if os.path.base(f)[:27] not in priority_instruments
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
            "error_test": False,
        }

        logger.info(f"Launching child flow: {run_name}")
        logger.info(f"configs: {config_json}")

        # run stream_ingest in parallel using AWS ECS fargate - this infrastructure is tied to
        # the prefect deployment
        if run_in_cloud:
            # asyncio.run(is_flowrun_running(run_name))
            run_deployment(
                name="stream-ingest/stream-ingest-deployment-v2",
                parameters=flow_params,
                flow_run_name=run_name,
                timeout=20,  # TODO timeout might need to be increase if we have race condition errors
            )

        # run stream_ingest in sequence on local
        else:
            # asyncio.run(is_flowrun_running(run_name))
            stream_ingest(**flow_params)

    logger.info("Parent flow complete")


if __name__ == "__main__":
    run_stream_ingest()
