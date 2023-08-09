import os
import json
import itertools as it
import numpy as np
import pandas as pd
import dask
import yaml
import datetime
import zarr
import fsspec
import re
from loguru import logger

from data_vent.producer import get_toc
from data_vent.metadata.util import compile_instrument_streams, compile_streams_parameters


def get_ooi_streams_and_parameters(instruments=None):
    if not instruments:
        instruments = get_toc()['instruments']
    streams_list = compile_instrument_streams(instruments)
    parameters_list = compile_streams_parameters(streams_list)
    streams = [
        dict(
            parameter_ids=','.join([str(p['pid']) for p in st['parameters']]),
            **st,
        )
        for st in streams_list
    ]
    streams_df = pd.DataFrame(streams).drop('parameters', axis=1)
    parameters_df = pd.DataFrame(parameters_list)
    return streams_df, parameters_df