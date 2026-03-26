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
(Updated 03/26/2026)

Example scenario: `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record` zarr needs a complete refresh.  
1) **Request new data from m2m through the RCA data harvest.**
   - Log into the prefect2 dashboard with an authorized uw email address. Click on deployments. In the deployments list click on `run_stream_ingest_2vcpu_16gb`. It may be on page 2 of the list.
   - In the upper right click the `run` button and on the dropdown select `custom run`.
   - Enter a run name. I usually tag the initial run with the stream name and `_REQUEST_DATA`.
   - **Make sure `force_harvest` and `refresh` parameters are toggled `ON`.**
   - Hit submit in the lower right.
  
3) **If necessary, get data you requested in step 2**
   - The initial prefect run will wait 30 minutes for m2m to serve the requested data. If the stream is dense it will likey take longer than this and you will see your initial request fails with a `data not ready error`
   - In this case wait 2-24 hours and make a new request. In prefect deployments once again click `run_stream_ingest_2vcpu_16gb`, and select `custom run`.
   - Enter a run name, for subsequent runs I usually tag them `_GET_DATA` and the stream name.
   - **`force harvest` should be toggled `OFF` when getting data that has already been requested, `refresh` should be toggled `ON`**
   - Hit submit, data should be ready and the run will proceed to concetenate the .nc files to zarr.

4) Confirm daily `stream_ingest` and `qaqc_pipeline_flow` are working as intended with the refreshed zarr of `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record`.

5) Recalculated advanced QAQC products can be found in the `rca-advanced-qaqc` S3 bucket. These products will mirror the refreshed zarr
dimensions and data, with added arrays containing advanced QAQC results.

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

