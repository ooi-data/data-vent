from typing import Any, Dict, Optional

import datetime
import json
import os 
import yaml 
from dateutil import parser
from pathlib import Path
from pydantic import BaseModel
from prefect import flow, get_run_logger

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
)

from data_vent.test_configs import FlowParameters, my_params
from data_vent.settings.main import harvest_settings
from data_vent.pipelines.notifications import github_issue_notifier

from data_vent.test_configs import DEV_PATH_SETTINGS

@flow
# TODO need to make sure these parameters are equivilant to prefect 1.0 version 
def stream_ingest(
    default_params: dict, 
    harvest_options: Dict[str, Any] = {},
    issue_config: Dict[str, Any] = {},
    force_harvest: bool = False,
    max_data_chunk: str = "100MB",
    error_test: bool = False,
    target_bucket: str = "s3://ooi-data",
):
    logger = get_run_logger()
    logger.info("Running a toy flow!?")

    # TODO automated github things - are these all deprecated??
    # default_gh_org = harvest_settings.github.data_org
    # issue_config.setdefault("gh_org", default_gh_org)
    # state_handlers = [github_issue_notifier(**issue_config)]
    
    # Check default_params
    if isinstance(default_params, dict):
        default_params = FlowParameters(**default_params)

    # Sets the defaults for flow config
    #config_required = False
    # if default_params.config is None:
    #     config_required = True

    default_dict = default_params.dict()

    #config = Parameter("config", required=config_required, default=default_dict.get("config", no_default),)
    #harvest_options = Parameter("harvest_options", default={})

    stream_harvest = get_stream_harvest(default_dict.get("config"), harvest_options)
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
            max_data_chunk,
            error_test,
            # TODO figure out what to do with task args
            # task_args={
            #     "state_handlers": state_handlers,
            # },
        )
        


stream_ingest(my_params)