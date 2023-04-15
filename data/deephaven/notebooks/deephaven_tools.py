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


def query_clickhouse(query):
    with clickhouse_connect.get_client(host='clickhouse', username='default', password='password', port=8123) as client:
        return dhpd.to_table(client.query_df(query))


def get_partitions(org_table, partition_by: str):
    partitioned_table = org_table.partition_by([partition_by])
    keys_table = partitioned_table.table.select_distinct(
        partitioned_table.key_columns)  # a 1 column DH table of unique keys
    iterator = keys_table.j_object.columnIterator(partition_by)  # this is a Java iterator
    keys_list = []
    while iterator.hasNext():
        keys_list.append(iterator.next())

    return partitioned_table, sorted(keys_list)


def get_ticks(symbols: list, n_ticks=10000):
    symbol_filter = ",".join([f"'{x}'" for x in symbols])

    query_ticks = f"""
    SELECT * FROM cryptofeed.trades
    WHERE
      symbol in ({symbol_filter})
    ORDER BY ts desc
    LIMIT {n_ticks}
    """
    return query_clickhouse(query_ticks)


def get_candles(symbols: list):
    symbol_filter = ",".join([f"'{x}'" for x in symbols])

    query_candles = f"""
    SELECT 
      symbol,
      toStartOfFiveMinute(ts)                       AS candle_st,
      argMin(price, ts)                             AS openp,
      max(price)                                    AS highp,
      min(price)                                    AS lowp,
      argMax(price, ts)                             AS closep,
      sum(price*size) / sum(size)                   AS vwap,
      sum(size)                                     AS volume,
      sum(if(side='buy', size, 0))                  AS volume_buys,
      sum(if(side!='buy', size, 0))                 AS volume_sells,
      toInt64(count(symbol))                        AS num_ticks,
      pow(log(highp/lowp), 2) / (4*log(2)) * 10000  AS vola_pk
    FROM cryptofeed.trades
    WHERE
      symbol in ({symbol_filter})
    GROUP BY symbol, candle_st
    ORDER BY candle_st ASC, symbol ASC
    """
    return query_clickhouse(query_candles)
