# data_vent

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
`flow.py` contains the complete orchestration and logic of the harvest process

# Infrastructure
## Flow deployment
Flows are deployed using the helper script `deploy.py`. This script can be run in a pre-configured environment
using the github workflow <Deploy Flows> associated with the repo. Deployment infrastructure specification can 
be found in `blocks.py` and needs to be updated and run manually. 

#TODO migrate to prefect worker orchestration model in place of agent model
## Prefect cloud server 
Deployed prefect flows are hosted on the free prefect2 cloud service, in this workspace: 
jdupreyuwedu/ooi-rca-prefect2
The data harvest is scheduled to run once daily at 6am PST. It has been scheduled using the prefect2 GUI.

## Prefect agent 
The prefect2 agent is hosted on an OOI-RCA AWS account EC2 instance. `molamola-prefect2-agent`
This agent was set up in-part following this tutorial: 
https://discourse.prefect.io/t/how-to-deploy-a-prefect-2-0-agent-to-an-ec2-instance-as-your-execution-layer/551

The agent environment is managed using miniconda, with the following required packages:

prefect==#TODO

prefect-aws==#TODO

On reboot the agent process can be restarted with `supervisord -c ./supervisord.conf`

#TODO have the process restart automatically on instance reboot 

# Development Environment 
With data-vent as cwd:

`conda create --name data-vent --clone base`

`conda activate data-vent`

`pip install -e .`
