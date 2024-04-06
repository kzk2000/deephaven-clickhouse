FROM ghcr.io/deephaven/server:0.33.3
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONPATH "${PYTHONPATH}:/data/storage/notebooks"

RUN pip install --no-cache-dir clickhouse-connect websockets orjson seaborn
RUN pip install --no-cache-dir deephaven-plugin-plotly-express deephaven-plugin-matplotlib deephaven-plugin-ui

COPY ./config/app.d /opt/deephaven/config/app.d
COPY ./config/deephaven.prop /opt/deephaven/config/deephaven.prop
