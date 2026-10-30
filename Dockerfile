FROM prefecthq/prefect:3-python3.11

COPY ./ /tmp/data_vent

RUN pip install uv
RUN uv pip install --system prefect-aws /tmp/data_vent