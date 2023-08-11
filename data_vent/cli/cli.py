from typing import Dict

from data_vent.test_configs import my_params
from data_vent.flow import stream_ingest

from loguru import logger
import typer

ooi_app = typer.Typer()

ooi_app.command('run')
def run(
        default_params: Dict = typer.Argument(
            my_params, help="passing my params to typer?"
        )
    ):
        "Runs joe's first typer app?"
        logger.info("Running stream-ingest!")
        stream_ingest(default_params=my_params)
        
        
