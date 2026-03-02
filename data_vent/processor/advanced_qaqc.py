import xarray as xr


def placeholder_qaqc_function(ds):
    param_da = ds["pco2_seawater"]

    test_da = xr.full_like(param_da, 1)

    ds["test_advanced_qaqc"] = test_da

    return ds