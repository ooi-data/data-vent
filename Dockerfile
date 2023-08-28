
FROM prefecthq/prefect:2-python3.10

COPY ./ /tmp/data_vent

RUN pip install -e /tmp/data_vent

# RUN mv /tmp/hello_planet/flow_configs /opt/flow_configs
