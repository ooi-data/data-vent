import click
import fsspec
import json
import requests

import pandas as pd

from data_vent.settings.main import harvest_settings
from loguru import logger

ANNOTATIONS_ENDPOINT = "https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find"
FS = fsspec.filesystem('s3', **harvest_settings.storage_options.aws.dict())
PROD_BUCKET = "ooi-data-prod"


def request_annotations(refdes):

    username = harvest_settings.ooi_config.username
    token = harvest_settings.ooi_config.token

    params = {"refdes": refdes}
    response = requests.get(ANNOTATIONS_ENDPOINT, params=params, auth=(username, token))

    if response.status_code == 200:
        response_json = json.loads(response.text)
    else:
        raise requests.exceptions.HTTPError(f"Request failed with status code: {response.status_code}")

    json_string = json.dumps(response_json)
    logger.info(f"annotation requested for {refdes}")

    return json_string

@click.command()
def harvest_annotations():

    priority_df = pd.read_csv(
        'https://raw.githubusercontent.com/OOI-CabledArray/rca-data-tools/main/rca_data_tools/qaqc/params/sitesDictionary.csv'  # noqa
    )
    priority_instruments = sorted(priority_df.refDes.unique())

    for instrument in priority_instruments: # ie: "CE04OSPS-SF01B-2B-PHSENA108"
        annotation_json = request_annotations(instrument)

        with FS.open(f"s3://{PROD_BUCKET}/annotations/{instrument}.json", "w") as s3_file:
            s3_file.write(annotation_json)
        logger.info(f"annotation saved to s3 for {instrument}")
    

if __name__ == "__main__":
    harvest_annotations()
