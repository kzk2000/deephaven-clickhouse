FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-plotly @deephaven/js-plugin-plotly-express @deephaven/js-plugin-matplotlib @deephaven/js-plugin-ui

FROM ghcr.io/deephaven/server:0.33.0
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONPATH "${PYTHONPATH}:/data/storage/notebooks"

RUN pip install --no-cache-dir clickhouse-connect websockets orjson seaborn
RUN pip install --no-cache-dir deephaven-plugin-plotly deephaven-plugin-plotly-express deephaven-plugin-matplotlib deephaven-plugin-ui

COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/

COPY ./config/app.d /opt/deephaven/config/app.d
COPY ./config/deephaven.prop /opt/deephaven/config/deephaven.prop
