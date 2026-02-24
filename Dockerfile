FROM prefecthq/prefect:2-python3.10

COPY ./ /tmp/data_vent

RUN pip install uv
RUN uv pip install --system prefect-aws /tmp/data_vent