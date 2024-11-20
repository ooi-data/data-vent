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
1) _bold_

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
