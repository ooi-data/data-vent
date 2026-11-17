# data-vent

[![Cabled Array Official](https://tinyurl.com/ca-official)](#)
[![Annotations Harvest](https://github.com/ooi-data/data-vent/actions/workflows/annotations-harvest.yaml/badge.svg)](https://github.com/ooi-data/data-vent/actions/workflows/annotations-harvest.yaml)
[![Metadata Harvest](https://github.com/ooi-data/data-vent/actions/workflows/metadata-harvest.yaml/badge.svg)](https://github.com/ooi-data/data-vent/actions/workflows/metadata-harvest.yaml)

# Overview
This repo contains code migrated from OOI-RCA data-harvest and ooi-harvester.

https://github.com/ooi-data/ooi-harvester

https://github.com/ooi-data/data-harvest

This migration has two aims:
* Separation of CAVA and OOI code bases and functionalities
* Upgrade to Prefect 2.0 for orchestration 

## Ownership
New code goes in the repo this rule points to:

* **data-vent** owns everything that talks to OOINet and writes data stores (`ooi-data`, `rca-advanced-qaqc`).
* **[rca-data-tools](https://github.com/OOI-CabledArray/rca-data-tools)** owns the instrument registry (`rca_data_tools/qaqc/params/`) and everything that reads stores and renders.

data-vent reads the registry from its installed rca-data-tools: the daily harvest list (`HARVEST_STREAMS`, from
`params/harvest.yaml`), ECS sizes, coordinate padding and the harvest-time calculations.

This repo's structure has been copied over from ooi-harvester. 
`tasks.py` contains the harvest's workhorse functions decorated as prefect @tasks 
`flow.py` contains the complete orchestration and logic of the harvest process.

# Infrastructure
## Flow deployment
Flows are deployed to prefect 2 push work pools using the prefect cli `prefect deploy`. 
For more details on prefect push pools vs workers see this documention:
https://docs.prefect.io/latest/guides/deployment/push-work-pools/.
Note: in the future this could be automated with a github workflow.

# How to refresh a UW RCA zarr file for a given stream 
(Updated 11/17/2026)

Example scenario: `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record` zarr needs a complete refresh.  
1) **Launch the refresh.**
   - In the Prefect dashboard, open the `run_stream_ingest_2vcpu_16gb` deployment, then `run` > `custom run`.
   - Set `streams` to the table name, e.g. `["CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record"]`. Left empty, the run covers every daily stream (rca-data-tools `params/harvest.yaml`).
   - Toggle **`refresh` ON**. Leave `force_harvest` OFF.

2) **That's it.** The run requests the full series from m2m and waits ~40 minutes for it.
   - Ready in time: the run rewrites the zarr.
   - Not ready: the run ends `Completed` ("Refresh requested..."). The hourly `refresh-sweeper` flow launches the
     processing run once m2m has the request ready, or after 2 days, when the run settles the timeout.
   - Daily appends skip the stream until the refresh finalizes ("Refresh pending; skipping append"), then pick up
     from the refreshed end date.

`refresh-sweeper` launches one processing run per request. If that run fails, the sweeper ends `Failed` each hour,
naming the stream, until it's handled:
   - Fix the cause, then relaunch with `refresh` ON to process the same request.
   - Or relaunch with `refresh` and `force_harvest` ON to drop the pending request and make a new one.

3) Confirm daily `stream_ingest` and `qaqc_pipeline_flow` are working as intended with the refreshed zarr of `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record`.

4) Recalculated advanced QAQC products can be found in the `rca-advanced-qaqc` S3 bucket. These products will mirror the refreshed zarr
dimensions and data, with added arrays containing advanced QAQC results. Each daily append also recomputes the last
2 days of results in place (`ADVANCED_QAQC_TAIL` in `tasks.py`).

# Development
`data-vent` has a direct dependency on the latest commit of `rca-data-tools` main branch. This means that if you want changes 
to `rca-data-tools` to be reflected in production `data-vent` you need to rebuild the `data-vent` image. To accomplish
this navigate to the `data-vent` directory and run the following commands.

```
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
docker buildx build --platform linux/amd64,linux/arm64 -t public.ecr.aws/p0l4c7i2/data-vent:latest --push .
```


With data-vent as cwd:

`conda create --name data-vent python=3.11 pip`

`conda activate data-vent`

`pip install -e .`

Linting uses black to 95 line length:
`black --line-length 95 data_vent/`

