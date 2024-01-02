import clickhouse_connect
import deephaven
import deephaven.arrow as dhpa
import deephaven.numpy as dhnp
import jpy
from deephaven import agg
from deephaven.table import Table
from deephaven.table_factory import ring_table
from typing import List, Union

JRingTableTools = jpy.get_type('io.deephaven.engine.table.impl.sources.ring.RingTableTools')
JsonNode = deephaven.dtypes.DType('com.fasterxml.jackson.databind.JsonNode')


def blink_tail_by(blink_table: Table, num_rows: int, by: Union[str, List[str]]) -> Table:
    """
    Transform blink_table to ring_table that keeps num_rows by <group>
    Workaround as suggested in https://github.com/deephaven/deephaven-core/issues/4309
    
    """
    return (
        blink_table.without_attributes(["BlinkTable"])
        .partitioned_agg_by(aggs=[], by=by, preserve_empty=True)
        .transform(lambda t: ring_table(t, num_rows))
        .merge()
    )


def get_first_ts(ticking_table):
    first_ts = ticking_table.agg_by([agg.min_('ts')])
    first_ts_py = dhnp.to_numpy(first_ts)[0,0]
    return first_ts_py


def query_clickhouse(query):
    with clickhouse_connect.get_client(host='clickhouse', username='default', password='password', port=8123) as client:
        return dhpa.to_table(client.query_arrow(query))


def query_clickhouse_df(query):
    with clickhouse_connect.get_client(host='clickhouse', username='default', password='password', port=8123) as client:
        return client.query_df(query)


def get_partitions(org_table, partition_by: str):
    partitioned_table = org_table.partition_by([partition_by])
    keys_table = partitioned_table.table.select_distinct(partitioned_table.key_columns)  # a 1 column DH table of unique keys
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
    return query_clickhouse(query_ticks).reverse()


def get_candles(symbols: list, n_rows=100, freq='5 minute'):
    symbol_filter = ",".join([f"'{x}'" for x in symbols])

    query_candles = f"""
    SELECT 
        symbol,
        toDateTime64(toStartOfInterval(ts, INTERVAL {freq}),9)  AS candle_st,
        argMin(price, ts)                                       AS openp,
        max(price)                                              AS highp,
        min(price)                                              AS lowp,
        argMax(price, ts)                                       AS closep,
        sum(price*size) / sum(size)                             AS vwap,
        -- avg(closep) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_closep
        closep/openp - 1                                        AS ret_o2c,
        -- closep/prev_closep - 1                                AS ret c2c,
        sum(size)                                               AS volume,
        sum(if(side='buy', size, 0))                            AS volume_buys,
        sum(if(side!='buy', size, 0))                           AS volume_sells,
        toInt64(count(symbol))                                  AS num_ticks,
        pow(log(highp/lowp), 2) / (4*log(2)) * 10000            AS vola_pk_bps
    FROM cryptofeed.trades
    WHERE
        symbol in ({symbol_filter})
    GROUP BY symbol, candle_st
    ORDER BY candle_st DESC, symbol ASC
    LIMIT {int(abs(n_rows))}
    """
    return query_clickhouse(query_candles).reverse()

