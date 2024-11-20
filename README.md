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
(Updated 11/20/2024)
Example scenario: `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record` zarr needs a complete refresh.
1) **Delete harvest metadata and zarr for `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record`.**
   - Navigate to RCA s3 `flow_process_bucket/harvest_status`. Delete the `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record` folder using the s3 GUI
   - Navigate to RCA s3 `flow_process_bucket/ooinet-requests`. Search for the prefix `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record` delete all requests containing this prefix
   - Navigate to RCA s3 `ooi-data` and delete the folder `CE04OSPS-SF01B-4F-PCO2WA102-streamed-pco2w_a_sami_data_record`. This is the actual zarr store.

# Known Issues
Refreshing streams is not working as intended. Setting `refresh=True` and `force_harvest=True` will only refresh 
the entire time series if all status files related to that stream are deleted first in `flow-process-bucket`.

# Development Environment 
With data-vent as cwd:

`conda create --name data-vent python=3.11 pip`

`conda activate data-vent`

`pip install -e .`

Linting uses black to 95 line length:
`black --line-length 95 data_vent/`
