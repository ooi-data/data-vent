"""
This module contains functions helpful for checking and validating 
array data throughout the harvest process.
"""
import xarray as xr
import numpy as np

from prefect import get_run_logger

from data_vent.exceptions import DuplicateTimeStampError


def check_for_timestamp_duplicates(ds: xr.DataArray) -> None:
    logger = get_run_logger()
    logger.info("entering check_for_timestamp_duplicates")

    timestamps_sorted = np.sort(ds.time.values)
    duplicate_indices = np.where(timestamps_sorted[1:] == timestamps_sorted[:-1])[0]

    if len(duplicate_indices) > 0:
        first_duplicate = ds.time.values[min(duplicate_indices)]
        last_duplicate = ds.time.values[max(duplicate_indices)]

        message = f"There are {len(duplicate_indices)} duplicate time stamps between {first_duplicate} and {last_duplicate}."
        logger.error(message)
        raise DuplicateTimeStampError(message)

    else:
        logger.info("No duplicate timestamps found.")
