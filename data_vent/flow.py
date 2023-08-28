from typing import Any, Dict, Optional

import datetime
import json
import os 
import yaml 
from dateutil import parser
from pathlib import Path
from pydantic import BaseModel
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

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
    data_availability
)

from data_vent.test_configs import FlowParameters, my_params
from data_vent.settings.main import harvest_settings
from data_vent.pipelines.notifications import github_issue_notifier

from data_vent.test_configs import DEV_PATH_SETTINGS

@flow
# TODO need to make sure these parameters are equivilant to prefect 1.0 version 
def stream_ingest(
    config: Dict,
    harvest_options: Dict[str, Any] = {},
    #issue_config: Dict[str, Any] = {},
    force_harvest: bool = False,
    max_chunk: str = "100MB",
    error_test: bool = False,
    target_bucket: str = "s3://ooi-data",
    export_da: bool = False, # TODO at least for testing
    gh_write_da: bool = False # TODO at least for testing
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
        max_chunk=max_chunk,
        export_da=export_da,
        gh_write_da=gh_write_da,
        error_test=error_test,
    )

    # Sets the defaults for flow config
    #config_required = False
    # if default_params.config is None:
    #     config_required = True

    flow_dict = flow_params.dict()

    #config = Parameter("config", required=config_required, default=default_dict.get("config", no_default),)
    #harvest_options = Parameter("harvest_options", default={})

    stream_harvest = get_stream_harvest(flow_dict.get("config"), harvest_options)
    # TODO how do you actually set these path settings - it is confusing
    stream_harvest.harvest_options.path_settings = DEV_PATH_SETTINGS['aws']

    is_requested = check_requested(stream_harvest)

    if is_requested == False:
    #with case(is_requested, False):
        # Run the data request here
        estimated_request = setup_harvest(
            stream_harvest,
            #TODO task_args necessary?
            # task_args={
            #     "state_handlers": state_handlers,
            # },
        )
        request_response = request_data(estimated_request, stream_harvest, force_harvest)

    if is_requested == True:
    #with case(is_requested, True):
        # Get request response directly here
        request_response = get_request_response(stream_harvest)
        # Now run the data check
        data_readiness = check_data(
            request_response,
            stream_harvest,
            # TODO task_args necessary?
            # task_args={
            #     "state_handlers": state_handlers,
            # },
        )

        response_json = get_response(data_readiness)
        stream_harvest = get_stream(data_readiness)

        # Process data to temp
        nc_files_dict = setup_process(response_json, target_bucket)
        stores_dict = data_processing(
            nc_files_dict,
            stream_harvest,
            max_chunk,
            error_test,
            # TODO figure out what to do with task args
            # task_args={
            #     "state_handlers": state_handlers,
            # },
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
            wait_for=final_path # TODO 
        )

        # in prefect 1.0 this sets the provided task as an upstream dependency of `availability` in this case
        # availability = set_upstream(final_path)

        #TODO I think this saves logs as files to s3 - will have to find alternative or alter for prefect 2.0?
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

@flow
def run_stream_ingest(
    test_run: bool=True,
    run_in_cloud: bool=True,
):

    logger = get_run_logger()
    logger.info("Starting parent flow...")

    config_dir = os.path.join(os.getcwd(), "flow_configs")


    if test_run:

        # TODO this are some "small data" streams for the sake of building out the new prefect 2 pipeline
        all_paths = [
            os.path.join(config_dir, 'CE04OSPS-SF01B-2B-PHSENA108', 'CE04OSPS-SF01B-2B-PHSENA108-streamed-phsen_data_record.yaml'),
            os.path.join(config_dir, 'CE04OSPS-SF01B-4F-PCO2WA102', 'CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record.yaml'),
            os.path.join(config_dir, 'CE04OSPS-SF01B-4A-NUTNRA102', 'CE04OSPS-SF01B-4A-NUTNRA102-streamed-nutnr_a_sample.yaml')
        ]
        logger.info(f"Using the following paths for this test run {all_paths}")

    else: # grabs all config yamls
        fs = fsspec.filesystem('')
        glob_path = config_dir + '/**/*.yaml'   
        logger.info(f"Searching for config yamls at path: {glob_path}")
        all_paths = fs.glob(glob_path)


    for config_path in all_paths:
        config_json = yaml.safe_load(Path(config_path).open())
        run_name = "-".join(
            [
                config_json['instrument'],
                config_json['stream']['method'],
                config_json['stream']['name'],
            ]
        )

        flow_params = {
        'config': config_json,
        'target_bucket': "s3://ooi-data",
        'max_chunk': "100MB",
        'export_da': False,
        'gh_write_da': False,
        'error_test': False,
        }

        logger.info(f"Launching child flow: {run_name}")
        logger.info(f"configs: {config_json}")

        # run stream_ingest in parallel on ECS instances?
        if run_in_cloud:
            run_deployment(
                name="stream-ingest/stream-ingest-deployment",
                parameters=flow_params,
                flow_run_name=run_name,
                timeout=10 #TODO what makes sense for this timeout, should the parent flow complete even if child flows fail?
            )
        
        # run stream_ingest in sequence on local
        else:
            stream_ingest(**flow_params)

    logger.warning("Parent flow complete - this is a test")


if __name__ == '__main__':
    run_stream_ingest()
