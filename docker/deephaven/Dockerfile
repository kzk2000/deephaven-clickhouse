FROM ghcr.io/deephaven/server-ui:0.37.1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONPATH "${PYTHONPATH}:/data/storage/notebooks"

RUN pip install --no-cache-dir clickhouse-connect websockets orjson seaborn

COPY ./config/app.d /opt/deephaven/config/app.d
COPY ./config/deephaven.prop /opt/deephaven/config/deephaven.prop
