FROM ghcr.io/deephaven/web-plugin-packager:latest as js-plugins
RUN ./pack-plugins.sh @deephaven/js-plugin-plotly @deephaven/js-plugin-plotly-express @deephaven/js-plugin-matplotlib

FROM ghcr.io/deephaven/server:0.27.1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONPATH "${PYTHONPATH}:/data/storage/notebooks"

RUN pip install --no-cache-dir clickhouse-connect==0.5.22
RUN pip install --no-cache-dir deephaven-plugin-plotly deephaven-plugin-plotly-express deephaven-plugin-matplotlib
COPY --from=js-plugins js-plugins/ /opt/deephaven/config/js-plugins/

COPY app.d /opt/deephaven/config/app.d
COPY deephaven.prop /opt/deephaven/config/deephaven.prop