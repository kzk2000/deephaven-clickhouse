FROM ghcr.io/deephaven/server:0.25.2
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONPATH "${PYTHONPATH}:/data/storage/notebooks"

RUN pip3 install clickhouse-connect==0.5.22

COPY app.d /opt/deephaven/config/app.d
COPY deephaven.prop /opt/deephaven/config/deephaven.prop
