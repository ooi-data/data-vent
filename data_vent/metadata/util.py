import os
import re
import json
import itertools as it

import dask
import dask.dataframe as dd
import fsspec
import gspread
from loguru import logger
import pandas as pd
import datetime
import requests
from lxml import etree
import numpy as np
from siphon.catalog import TDSCatalog
import zarr
from s3fs.core import S3FileSystem

#from ..config import STORAGE_OPTIONS
from data_vent.utils.compute import map_concurrency
#from ..utils.encoders import NumpyEncoder
from data_vent.utils.conn import fetch_streams #retrieve_deployments
#from ..utils.parser import get_items, rename_item, parse_dataset_element


INSTRUMENT_MAP = {
    'MOP': '3-Axis Motion Pack',
    'HYDGN0': 'Hydrogen Sensor',
    'VEL3D': '3-D Single Point Velocity Meter',
    'ADCP': 'ADCP',
    'FLOBN': 'Benthic Fluid Flow',
    'ZPLS': 'Bio-acoustic Sonar',
    'BOTPT': 'Bottom Pressure and Tilt',
    'HYDBB': 'Broadband Acoustic Receiver (Hydrophone)',
    'OBSBB': 'Broadband Ocean Bottom Seismometer',
    'METBK': 'Bulk Meteorology Instrument Package',
    'CTD': 'CTD',
    'TMPSF': 'Diffuse Vent Fluid 3-D Temperature Array',
    'CAMHD': 'HD Digital Video Camera',
    'CAM': 'Digital Still Camera',
    'FDCHP': 'Direct Covariance Flux',
    'DO': 'Dissolved Oxygen',
    'ENG': 'Engineering',
    'FL': 'Fluorometer',
    'HPIES': 'Horizontal Electric Field, Pressure and Inverted Echo Sounder',
    'THSPH': 'Hydrothermal Vent Fluid In-situ Chemistry',
    'RASFL': 'Hydrothermal Vent Fluid Interactive Sampler',
    'TRHPH': 'Hydrothermal Vent Fluid Temperature and Resistivity',
    'HYDLF': 'Low Frequency Acoustic Receiver (Hydrophone)',
    'MASSP': 'Mass Spectrometer',
    'NUTNR': 'Nitrate',
    'OSMOI': 'Osmosis-Based Water Sampler',
    'PPSDN': 'Particulate DNA Sampler',
    'PCO2A': 'pCO2 Air-Sea',
    'PCO2W': 'pCO2 Water',
    'PARAD': 'Photosynthetically Active Radiation',
    'PRESF': 'Seafloor Pressure',
    'PHSEN': 'Seawater pH',
    'OBSSP': 'Short-Period Ocean Bottom Seismometer',
    'VELPT': 'Single Point Velocity Meter',
    'SPKIR': 'Spectral Irradiance',
    'OPTAA': 'Spectrophotometer',
    'WAVSS': 'Surface Wave Spectra',
    'PREST': 'Tidal Seafloor Pressure',
    'COVIS': 'Vent Imaging Sonar',
    'AOA': 'A-0-A Pressure Sensor',
    'SCT': 'Self-Calibrating Triaxial Accelerometer',
    'SCP': 'Self-Calibrating Pressure Recorder',
    'D1000': 'Hydrothermal Vent Fluid Temperature Sensor',
}


def df2list(df):
    return json.loads(df.to_json(orient='records', date_format="iso"))


def set_instrument_group(rd):
    inst_code = rd.split('-')[3]
    m = re.search(('|'.join(list(INSTRUMENT_MAP.keys()))), inst_code)
    if m:
        return m.group()
    else:
        return None


def compile_instrument_streams(instruments):
    logger.info("Compiling instrument streams ...")
    streams_list = map_concurrency(fetch_streams, instruments, max_workers=50)
    streams_list = list(
        dict(
            table_name="-".join(
                [st["reference_designator"], st["method"], st["stream"]]
            ),
            **st,
        )
        for st in it.chain.from_iterable(streams_list)
    )
    streamsdf = pd.DataFrame(streams_list)
    streamsdf.loc[:, "beginTime"] = streamsdf["beginTime"].apply(
        pd.to_datetime
    )
    streamsdf.loc[:, "endTime"] = streamsdf["endTime"].apply(pd.to_datetime)
    streamsdf.loc[:, 'group_code'] = streamsdf.reference_designator.apply(
        set_instrument_group
    )
    return df2list(streamsdf)


def compile_streams_parameters(streams_list):
    logger.info("Compiling parameters ...")
    all_params = [
        params
        for params in it.chain.from_iterable(
            [st['parameters'] for st in streams_list]
        )
    ]
    return list({v['pid']: v for v in all_params}.values())