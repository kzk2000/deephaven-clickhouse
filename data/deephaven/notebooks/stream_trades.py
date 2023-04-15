import clickhouse_connect
import jpy
import time
import deephaven.dtypes as dht
import deephaven.pandas as dhpd
import deephaven.stream.kafka.consumer as ck
from deephaven import agg, merge
from deephaven.table import Table

# initialize global varibles 
JRingTableTools = jpy.get_type('io.deephaven.engine.table.impl.sources.ring.RingTableTools')
client = clickhouse_connect.get_client(host='clickhouse', username='default', password='password', port=8123)

# latest ticks from Kafka (init as ring table to hold latest ticks in memory until the 'final_ring' is created below)
trades_kafka = ck.consume(
  {'bootstrap.servers': 'redpanda:29092'},
  'trades',
  key_spec=ck.KeyValueSpec.IGNORE,
  value_spec = ck.json_spec([
      ('ts',         dht.DateTime),
      ('receipt_ts', dht.DateTime),
      ('symbol',     dht.string),
      ('exchange',   dht.string),
      ('side',       dht.string),
      ('size',       dht.double),
      ('price',      dht.double),
      ('trade_id',   dht.int_),
  ]),    
  table_type=ck.TableType.ring(10000))\
  .drop_columns(['KafkaPartition', 'KafkaTimestamp'])\
  .update_view('is_db = (long) 0')

trades_kafka.j_table.awaitUpdate()  # this waits for at least 1 tick before we continue below this line

# historical ticks from ClickHouse
print(trades_kafka.size)
first_ts = trades_kafka.agg_by([agg.min_('ts')])
first_ts_py = dhpd.to_pandas(first_ts).iloc[0,0]

query_history = f"""
  SELECT *, toInt64(1) as is_db FROM cryptofeed.trades
  WHERE 
    ts >= now() - INTERVAL 3 HOUR
    AND ts <= '{first_ts_py}'
  ORDER BY ts ASC
"""
trades_clickhouse = dhpd.to_table(client.query_df(query_history))


# merge history (ClickHouse) + latest (Kafka) into one table, the .last_by(['symbol', 'exchange', 'trade_id']) ensures no dups
final_stream = merge([trades_clickhouse, trades_kafka])
final_ring = Table(JRingTableTools.of(final_stream.j_table, 20000))\
  .last_by(['symbol', 'exchange', 'ts', 'KafkaOffset'])\
  .drop_columns(['KafkaOffset'])\
  .sort(['ts'])


snap = final_ring.snapshot()

gg = final_ring.agg_by(agg.count_('count'), by=['symbol', 'exchange'])


def get_partitions(org_table, partition_by: str):

    partitioned_table = org_table.partition_by([partition_by])
    keys_table = partitioned_table.table.select_distinct(partitioned_table.key_columns)  # a 1 column DH table of unique keys
    iterator = keys_table.j_object.columnIterator(partition_by)  # this is a Java iterator
    keys_list = []
    while iterator.hasNext():
        keys_list.append(iterator.next())

    return partitioned_table, sorted(keys_list)


partitioned_table, keys_list = get_partitions(final_stream, 'symbol')

lst = []
for index, key in enumerate(keys_list):
    table_now = partitioned_table.get_constituent([key])
    lst.append(table_now.tail(1000))

tail5 = Table(JRingTableTools.of(merge(lst).j_table, 4000))\
  .last_by(['symbol', 'exchange', 'trade_id'])\
  .drop_columns(['KafkaOffset'])\
  .sort(['ts'])


tail5 = final_ring.tail_by(300, by=['symbol'])
last_by_symbol = tail5.last_by('symbol')














# load data from ClickHouse
client = clickhouse_connect.get_client(host='ch_server', username='default', password='password', port=8123)

query = 'SELECT * FROM cryptofeed.trades order by ts desc'
kk = dhpd.to_table(client.query_df(query))

query_candles = """
SELECT 
  symbol,
  toStartOfFiveMinute(ts)       AS candle_st,
  argMin(price, ts)             AS openp,
  max(price)                    AS highp,
  min(price)                    AS lowp,
  argMax(price, ts)             AS closep,
  sum(price*size) / sum(size)   AS vwap,
  sum(size)                     AS volume,
  sum(if(side='buy', size, 0))  AS volume_buys,
  sum(if(side!='buy', size, 0)) AS volume_sells,
  toInt64(count(symbol))        AS num_ticks,
  pow(log(highp/lowp), 2) / (4*log(2)) * 10000  AS vola_pk
FROM cryptofeed.trades
WHERE
  symbol = 'ETH-USD'
GROUP BY symbol, candle_st
ORDER BY candle_st ASC, symbol ASC
"""
candles = dhpd.to_table(client.query_df(query_candles))


import deephaven.sql import execute_sql



