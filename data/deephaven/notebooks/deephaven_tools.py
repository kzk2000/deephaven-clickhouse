import clickhouse_connect
import deephaven.pandas as dhpd
import deephaven
import json
import jpy
import numpy as np
from deephaven.table import Table

JRingTableTools = jpy.get_type('io.deephaven.engine.table.impl.sources.ring.RingTableTools')
JsonNode = deephaven.dtypes.DType('com.fasterxml.jackson.databind.JsonNode')


def make_ring(t: Table, size: int) -> Table:
    return Table(j_table=JRingTableTools.of(t.j_table, size))


def run_query_clickhouse(query):
    with clickhouse_connect.get_client(host='clickhouse', username='default', password='password', port=8123) as client:
        return dhpd.to_table(client.query_df(query))
