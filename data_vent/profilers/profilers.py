# psuedocode for creating profiler data products 

# cli, prefect decorators for observability ==== to run after data harvest, ideally before plotting scripts
# for a given reference designator and parameter ie CE04OSPS-SF01B-2A-CTDPFA107 sea_water_temperature 
# load the data from zarr CE04OSPS-SF01B-2A-CTDPFA107-streamed-ctdpf_sbe43_sample
# load profilers from https://github.com/OOI-CabledArray/profileIndices/blob/main/CE04OSPS_profiles_2025.csv 
# use zarr end date to see what new profiles are avialable OR process entire timeseries 
# for each profile use timeslice for start and peak (upcast), unless ph and pco2 (downcast)...then it uses peak and end
# create xarray indexed by profile number and depth, so it can be in the same matrix
# each profile will also have start, peak, end metadata, along with additional metadata based on data requirements 
# consider including a profile "midpoint" halfway between start and peak/ peak and end??
# this will will be saved to a zarr store CE04OSPS-SF01B-2A-CTDPFA107-regridded-sea_water_temperature or something similar

# then a seperate script will be responsible for taking all parameters at a given location, and merging them into a single zarr store 
# with consolidated metadata - this will avoid concurrent writes in the above process? 
