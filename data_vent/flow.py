import datetime
import json
from typing import Any, Dict, Optional, List

import fsspec
from dateutil import parser
from pydantic import BaseModel
from prefect import flow, get_run_logger
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterName,
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateType,
    FlowRunFilterTags,
)
from prefect.client.schemas.objects import StateType
from prefect.deployments import run_deployment
from prefect.states import Completed, Failed

from data_vent.exceptions import DataNotReadyError
from data_vent.processor.checker import check_in_progress
from data_vent.producer.models import HarvestStatus

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
    read_status_json,
    run_advanced_qaqc,
)

from rca_data_tools.qaqc.utils import ecs_job_variables, load_site_calculations
from rca_data_tools.qaqc.constants import PARAMS_DIR, HARVEST_STREAMS, HARVEST_COMPUTE

from data_vent.config import STORAGE_OPTIONS, DATA_BUCKET, FLOW_PROCESS_BUCKET


def stream_config(table_name: str) -> Dict[str, Any]:
    """stream_ingest config for a table name: <27-char refDes>-<method>-<stream>"""
    method, name = table_name[28:].split("-", 1)
    return {
        "instrument": table_name[:27],
        "stream": {"method": method, "name": name},
        "harvest_options": {"path": f"s3://{DATA_BUCKET}"},
    }


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
    harvest_calc_dict = load_site_calculations(PARAMS_DIR / 'siteCalculations.csv', during_harvest=True)

    # A refresh requests the full series once, then processes that request when OOINet has it
    # ready: in this run if it's ready within check_data's retries, else via refresh-sweeper.
    # force_harvest abandons a pending request for a new one.
    if refresh:
        new_request = force_harvest or not stream_harvest.status.refresh_pending
        logger.info(
            "Refresh: requesting the full series" if new_request
            else f"Refresh: processing the request made {stream_harvest.status.requested_at}"
        )
        reset_status_json(stream_harvest, new_request)

    is_requested = check_requested(stream_harvest)

    while not is_requested:
        # Run the data request here
        estimated_request = setup_harvest(stream_harvest)

        # a new refresh request always goes to OOINet, never this month's cached one
        request_response = request_data(estimated_request, stream_harvest, force_harvest or refresh)

        logger.info("Confirming data has been succesfully requested")
        read_status_json(stream_harvest)

        is_requested = check_requested(stream_harvest)

    if is_requested == "SKIPPED":
        return Completed(message="Skipping harvest. No new data needed.")
    if is_requested == "REFRESH_PENDING":
        return Completed(message="Refresh pending; skipping append.")

    logger.info("Data succesfully requested - checking data readiness")
    # Get request response directly here
    request_response = get_request_response(stream_harvest)
    try:
        data_readiness = check_data(request_response, stream_harvest)
    except DataNotReadyError:
        if not refresh:
            raise
        return Completed(message="Refresh requested; refresh-sweeper processes it once OOINet has it ready.")

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
    finalize_data_stream(
        stores_dict,
        stream_harvest,
        max_chunk,
    )

    if stream_harvest.table_name in harvest_calc_dict:
        run_advanced_qaqc(
            stream_harvest,
            nc_files_dict,
            refresh,
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
        streams (Optional[List[str]]): table names to harvest, e.g.
            `RS01SBPS-PC01A-4A-CTDPFA103-streamed-ctdpf_optode_sample` (archived streams too).
            Defaults to rca-data-tools `HARVEST_STREAMS` (params/harvest.yaml).
        run_in_cloud (bool): If true, harvesters run in parallel on AWS Fargate instances orchestrated
            by a prefect deployment. Set to false to run harvesters in series on local machine. This
            can be useful for debugging.
        force_harvest (Optional[bool]): if `True` force pipeline to make harvest request of m2m. With
            `refresh`, drops a pending refresh request for a new one.
        refresh (Optional[bool]): rewrite the stream from t0. Requests the full series, or processes the
            pending request; refresh-sweeper finishes requests that aren't ready in this run.
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

    streams = streams or HARVEST_STREAMS
    logger.info(f"Running {len(streams)} streams")

    for run_name in streams:
        flow_params = {
            "config": stream_config(run_name),
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
        if run_in_cloud:
            launch_stream_ingest(run_name, flow_params)
        # run stream_ingest in sequence on local
        else:
            stream_ingest(**flow_params)

    logger.info("Parent flow complete")


def launch_stream_ingest(run_name: str, flow_params: Dict[str, Any], tags: Optional[List[str]] = None):
    """Run stream_ingest on AWS ECS Fargate, sized per run from harvest.yaml."""
    logger = get_run_logger()
    size = HARVEST_COMPUTE.get(run_name, {}).get("refresh" if flow_params["refresh"] else "append", "2vcpu_16gb")
    if size != "2vcpu_16gb":
        logger.warning(f"{run_name} requires additional compute: {size}")
    run_deployment(
        name="stream-ingest/stream_ingest",
        parameters=flow_params,
        flow_run_name=run_name,
        job_variables=ecs_job_variables(size),
        tags=tags,
        timeout=20,  # timeout can be adjusted based on AWS cluster race conditions
    )


SWEEPER_TAG = "refresh-sweeper"
REFRESH_TIMEOUT = datetime.timedelta(days=2)  # check_data's request timeout


def _pending_refreshes() -> Dict[str, Dict[str, Any]]:
    """Status json of each stream with a refresh request out, by table name."""
    fs = fsspec.filesystem("s3", **STORAGE_OPTIONS["aws"])
    files = [f["name"] for f in fs.ls(f"{FLOW_PROCESS_BUCKET}/harvest-status/", detail=True) if f["type"] == "file"]
    statuses = {p.rsplit("/", 1)[-1]: HarvestStatus(**json.loads(b)) for p, b in fs.cat(files).items()}
    return {table: s for table, s in statuses.items() if s.refresh_pending}


def _stream_ingest_runs():
    """stream_ingest runs still in flight, and the sweeper's own launches from the last week."""
    active_states = [StateType.SCHEDULED, StateType.PENDING, StateType.RUNNING, StateType.PAUSED]
    with get_client(sync_client=True) as client:
        active = client.read_flow_runs(
            flow_run_filter=FlowRunFilter(state=FlowRunFilterState(type=FlowRunFilterStateType(any_=active_states))),
            deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=["stream_ingest"])),
            limit=200,
        )
        launched = client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                tags=FlowRunFilterTags(all_=[SWEEPER_TAG]),
                start_time=FlowRunFilterStartTime(after_=datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)),
            ),
            limit=200,
        )
    return active, launched


def _run_table(flow_run) -> str:
    c = flow_run.parameters["config"]
    return f"{c['instrument']}-{c['stream']['method']}-{c['stream']['name']}"


@flow
def refresh_sweeper():
    """
    Hourly. For each stream with a refresh request out, launch its processing run (refresh=True)
    once OOINet has the request ready, or once it passes the 2-day timeout so check_data settles
    it. Each request gets one automatic launch; if that run fails the sweeper ends Failed every
    hour until someone relaunches the refresh.
    """
    logger = get_run_logger()
    pending = _pending_refreshes()
    if not pending:
        logger.info("No refreshes pending.")
        return
    active, launched = _stream_ingest_runs()
    in_flight = {_run_table(r) for r in active}
    now = datetime.datetime.now(datetime.timezone.utc)

    stuck = []
    for table, status in pending.items():
        requested = parser.parse(status.requested_at).replace(tzinfo=datetime.timezone.utc)
        age = now - requested
        failed = [r for r in launched if _run_table(r) == table and r.start_time and r.start_time > requested
                  and r.state_type in (StateType.FAILED, StateType.CRASHED)]
        if table in in_flight:
            logger.info(f"{table}: a run is in flight.")
        elif failed:
            logger.error(f"{table}: refresh processing run {failed[0].name} ({failed[0].id}) ended {failed[0].state_name}. Fix and relaunch with refresh=True.")
            stuck.append(table)
        elif age > REFRESH_TIMEOUT or not _in_progress(status):
            logger.info(f"{table}: request from {status.requested_at} is ready, launching processing.")
            launch_stream_ingest(table, {"config": stream_config(table), "refresh": True}, tags=[SWEEPER_TAG])
        else:
            logger.info(f"{table}: waiting on OOINet ({age} since request).")

    if stuck:
        return Failed(message=f"Refresh processing failed for {', '.join(stuck)}")


def _in_progress(status: HarvestStatus) -> bool:
    """OOINet is still building the request. A missing response file counts as ready, so the
    launched run hits get_request_response, which clears it."""
    try:
        with fsspec.open(status.data_response, **STORAGE_OPTIONS["aws"]) as f:
            status_url = json.load(f)["result"]["status_url"]
    except FileNotFoundError:
        return False
    return check_in_progress(status_url)


if __name__ == "__main__":
    run_stream_ingest()
