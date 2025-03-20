import datetime
import math

import zarr
import numpy as np
from loguru import logger
import fsspec
import dask
import xarray as xr
from xarray import coding

from rechunker.algorithm import prod
from rechunker import rechunk  # noqa

from .utils import (
    _reindex_zarr,
    _prepare_existing_zarr,
    _prepare_ds_to_append,
    _validate_dims,
)


def _update_time_coverage(store: fsspec.mapping.FSMap) -> None:
    """Updates start and end date in global attributes"""
    zg = zarr.open_group(store, mode="r+")
    calendar = zg.time.attrs.get("calendar", "gregorian")
    units = zg.time.attrs.get("units", "seconds since 1900-01-01 0:0:0")
    start, end = xr.coding.times.decode_cf_datetime(
        [zg.time[0], zg.time[-1]], units=units, calendar=calendar
    )
    zg.attrs["time_coverage_start"] = str(start)
    zg.attrs["time_coverage_end"] = str(end)
    zarr.consolidate_metadata(store)
    return str(start), str(end)


def get_logger():
    from loguru import logger

    return logger


def is_zarr_ready(store):
    meta = store.get(".zmetadata")
    if meta is None:
        return False
    return True


def preproc(ds):
    logger.info("Preprocessing dataset...")
    if "obs" in ds.dims:
        rawds = ds.swap_dims({"obs": "time"}).reset_coords(drop=True)
    else:
        rawds = ds
    string_variables = []
    for v, var in rawds.variables.items():
        if (
            not np.issubdtype(var.dtype, np.number)
            and not np.issubdtype(var.dtype, np.datetime64)
            and not np.issubdtype(var.dtype, np.bool_)
            and "qartod_executed" not in v  # keep qartod_executed variable
        ):
            if not coding.strings.is_unicode_dtype(var.dtype) or var.dtype == object:
                string_variables.append(v)
    # Drop variables that contains strings.. not necessary data.
    logger.info(f"Removing variables containing strings: {','.join(string_variables)}")
    rawds = rawds.drop_vars(string_variables, errors="ignore")
    return rawds


def update_metadata(dstime, download_date, unit=None, extra_attrs={}):
    """Updates the dataset metadata to be more cf compliant, and add CAVA info"""

    for v in dstime.variables:
        var = dstime[v]
        att = var.attrs

        for i, j in att.items():
            if isinstance(j, list) or isinstance(j, np.ndarray):
                att.update({i: ",".join(map(str, j))})
        var.attrs.update(att)

        # unit modifier
        if "units" in var.attrs:
            units = var.attrs["units"]
            if isinstance(units, np.ndarray):
                unit = ",".join(units)
            else:
                unit = units

            # Fix celcius to correct cf unit
            if unit == "ºC":
                var.attrs["units"] = "degree_C"
            else:
                var.attrs["units"] = unit

        # ancillary variables modifier
        if "ancillary_variables" in var.attrs:
            ancillary = var.attrs["ancillary_variables"]
            anc_vars = " ".join(ancillary.split(","))
            var.attrs["ancillary_variables"] = anc_vars

        # long name modifier
        if "long_name" not in var.attrs:
            if "qc_executed" in v:
                var.attrs["long_name"] = "QC Checks Executed"
            if "qc_results" in v:
                var.attrs["long_name"] = "QC Checks Results"
            if v == "lat":
                var.attrs["long_name"] = "Location Latitude"
            if v == "lon":
                var.attrs["long_name"] = "Location Longitude"
            if v == "obs":
                var.attrs["long_name"] = "Observation"
            if v == "deployment":
                var.attrs["long_name"] = "Deployment Number"
            if v == "id":
                var.attrs["long_name"] = "Observation unique id"

    # Change preferred_timestamp data type
    # if "preferred_timestamp" in dstime.variables:
    #     dstime["preferred_timestamp"] = dstime["preferred_timestamp"].astype(
    #         "|S36"
    #     )

    dstime.attrs["comment"] = (
        "Some of the metadata of this dataset has been modified to be CF-1.6 compliant."
    )
    dstime.attrs["Notes"] = (
        "This netCDF product is a copy of the data available through the NSF Ocean Observatories Initiative."  # noqa
    )
    dstime.attrs["Owner"] = (
        "NSF, Ocean Observatories Initiative, Regional Cabled Array, University of Washington."  # noqa
    )
    dstime.attrs["date_downloaded"] = download_date
    dstime.attrs["date_processed"] = datetime.datetime.now().isoformat()

    # Add extra global attributes last!
    if extra_attrs:
        for k, v in extra_attrs.items():
            dstime.attrs[k] = v

    # Clean up metadata
    dstime = _meta_cleanup(dstime)

    return dstime


def _meta_cleanup(chunked_ds):
    if "time_coverage_resolution" in chunked_ds.attrs:
        del chunked_ds.attrs["time_coverage_resolution"]
    if "uuid" in chunked_ds.attrs:
        del chunked_ds.attrs["uuid"]
    if "creator_email" in chunked_ds.attrs:
        del chunked_ds.attrs["creator_email"]
    if "contributor_name" in chunked_ds.attrs:
        del chunked_ds.attrs["contributor_name"]
    if "contributor_role" in chunked_ds.attrs:
        del chunked_ds.attrs["contributor_role"]
    if "acknowledgement" in chunked_ds.attrs:
        del chunked_ds.attrs["acknowledgement"]
    if "requestUUID" in chunked_ds.attrs:
        del chunked_ds.attrs["requestUUID"]
    if "feature_Type" in chunked_ds.attrs:
        del chunked_ds.attrs["feature_Type"]

    return chunked_ds


def append_to_zarr(mod_ds, store, encoding, logger=None):
    if logger is None:
        logger = get_logger()
    existing_zarr = zarr.open_group(store, mode="a")
    existing_var_count = len(list(existing_zarr.array_keys()))
    to_append_var_count = len(mod_ds.variables)

    if existing_var_count < to_append_var_count:
        existing_zarr = _prepare_existing_zarr(store, mod_ds, enc=encoding)
    else:
        mod_ds = _prepare_ds_to_append(store, mod_ds)

    dim_indexer, modify_zarr_dims, issue_dims = _validate_dims(
        mod_ds, existing_zarr, append_dim="time"
    )

    if len(issue_dims) > 0:
        logger.warning(
            f"{','.join(issue_dims)} dimension(s) are problematic. Skipping append..."
        )
        return False

    if modify_zarr_dims:
        logger.info("Reindexing zarr ...")
        existing_zarr = _reindex_zarr(store, dim_indexer)
    elif dim_indexer and not modify_zarr_dims:
        logger.info("Reindexing dataset to append ...")
        mod_ds = mod_ds.reindex(dim_indexer)

    # Remove append_dim duplicates by checking for existing tail
    append_dim = "time"
    if existing_zarr[append_dim][-1] == mod_ds[append_dim].data[0]:
        mod_ds = mod_ds.drop_isel({append_dim: 0})

    if mod_ds[append_dim].size == 0:
        logger.warning("Nothing to append.")
    else:
        logger.info("Appending zarr file.")
        mod_ds.to_zarr(
            store,
            consolidated=True,
            compute=True,
            mode="a",
            append_dim=append_dim,
            safe_chunks=False,
        )

    return True


def _tens_counts(num: int, places: int = 2) -> int:
    return (math.floor(math.log10(abs(num))) + 1) - places


def _round_down(to_round: int) -> int:
    """Rounds down integers to whole zeros"""
    digit_round = 10 ** _tens_counts(to_round)
    return to_round - to_round % digit_round


def _calc_chunks(variable: xr.DataArray, max_chunk="100MB"):
    """Dynamically figure out chunk based on max chunk size"""
    max_chunk_size = dask.utils.parse_bytes(max_chunk)
    dim_shape = {x: y for x, y in zip(variable.dims, variable.shape) if x != "time"}
    if "time" in variable.dims:
        time_chunk = math.ceil(
            max_chunk_size / prod(dim_shape.values()) / variable.dtype.itemsize
        )
        dim_shape["time"] = _round_down(time_chunk)
    chunks = tuple(dim_shape[d] for d in list(variable.dims))
    return chunks


def chunk_ds(
    chunked_ds,
    max_chunk="100MB",
    time_max_chunks="100MB",
    existing_enc=None,
    apply=True,
):
    compress = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)

    if existing_enc is None:
        raw_enc = {}
        for k, v in chunked_ds.data_vars.items():
            chunks = _calc_chunks(v, max_chunk=max_chunk)

            # add extra encodings
            extra_enc = {}
            if "_FillValue" in v.encoding:
                extra_enc["_FillValue"] = v.encoding["_FillValue"]
            raw_enc[k] = dict(compressor=compress, dtype=v.dtype, chunks=chunks, **extra_enc)

        if "time" in chunked_ds:
            chunks = _calc_chunks(chunked_ds["time"], max_chunk=time_max_chunks)
            raw_enc["time"] = {"chunks": chunks}
    elif isinstance(existing_enc, dict):
        raw_enc = existing_enc
    else:
        raise ValueError("existing encoding only accepts dictionary!")

    # Chunk the actual xr dataset
    if apply:
        for k, v in chunked_ds.variables.items():
            encoding = raw_enc.get(k, None)
            var_chunks = {}
            if isinstance(encoding, dict):
                var_chunks = encoding["chunks"]

            chunked_ds[k] = chunked_ds[k].chunk(chunks=var_chunks)

    return chunked_ds, raw_enc
