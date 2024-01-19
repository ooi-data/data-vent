# data-vent
(working name)

[![Cabled Array Official](https://tinyurl.com/ca-official)](#)

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

## Prefect worker (deprecated 01/19/2024)
The prefect2 worker is hosted on an OOI-RCA AWS account ECS instance. `molamola-prefect2-worker`
This worker was set up in-part following this tutorial :
https://discourse.prefect.io/t/how-to-run-a-prefect-2-worker-as-a-systemd-service-on-linux/1450
`prefect` and `prefect-aws` are installed into the worker's global environment. The systemd 
service unite file which controls the prefect-worker service is located at `/etc/systemd/system`
An AMI (amazon machine image) exists of this worker `prefect2-worker-image` in the OOI-RCA
AWS account.

To check the status of the worker run `sudo systemctl status prefect-worker`

## Prefect cloud server (deprecated 01/19/2024)
Deployed prefect flows are hosted on the free prefect2 cloud service, in this workspace: 
jdupreyuwedu/ooi-rca-prefect2
The data harvest is scheduled to run once daily at 6am PST. It has been scheduled using the prefect2 GUI.

# Development Environment 
With data-vent as cwd:

`conda create --name data-vent python=3.11 pip`

`conda activate data-vent`

`pip install -e .`

Linting uses black to 95 line length:
`black --line-length 95 data_vent/`
