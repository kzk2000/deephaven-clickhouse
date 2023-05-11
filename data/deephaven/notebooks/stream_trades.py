import clickhouse_connect
import deephaven.dtypes as dht
import deephaven.pandas as dhpd
import deephaven.stream.kafka.consumer as ck
import deephaven_tools as tools
import jpy
from deephaven import agg, merge
from deephaven.table import Table
from deephaven.plot import PlotStyle
from deephaven.plot.figure import Figure
from deephaven.plot.selectable_dataset import one_click

# latest from Kafka (init as ring table to hold latest ticks in memory until the 'final_ring' is created below)
trades_kafka = ck.consume(
    {'bootstrap.servers': 'redpanda:29092'},
    'trades',
    key_spec=ck.KeyValueSpec.IGNORE,
    value_spec=ck.json_spec([
        ('ts', dht.DateTime),
        ('receipt_ts', dht.DateTime),
        ('symbol', dht.string),
        ('exchange', dht.string),
        ('side', dht.string),
        ('size', dht.double),
        ('price', dht.double),
        ('trade_id', dht.int_),
    ]),
    table_type=ck.TableType.ring(10000)) \
    .drop_columns(['KafkaPartition', 'KafkaTimestamp']) \
    .update_view('is_db = (long) 0')

trades_kafka.j_table.awaitUpdate()  # this waits for at least 1 tick before we continue below this line

# historical ticks from ClickHouse
query_history = f"""
  SELECT *, toInt64(1) as is_db FROM cryptofeed.trades
  WHERE 
    ts >= now() - INTERVAL 3 HOUR
    AND ts <= '{tools.get_first_ts(trades_kafka)}'
  ORDER BY ts ASC
"""
trades_clickhouse = tools.query_clickhouse(query_history)

# merge history (ClickHouse) + latest (Kafka) into one table, 
# the .last_by(['symbol', 'exchange', 'ts', 'KafkaOffset']) ensures no dups after stitching
trades_stream = merge([trades_clickhouse, trades_kafka])
trades = tools.make_ring(trades_stream, 20000) \
    .last_by(['symbol', 'exchange', 'ts', 'KafkaOffset']) \
    .drop_columns(['KafkaOffset']) \
    .sort(['ts'])

# snap = trades_ring.snapshot()  # for testing is_db switch from 0 to 1

tick_count_by_exch = trades.agg_by(agg.count_('count'), by=['symbol', 'exchange'])

last_trade = trades.last_by(['symbol']).sort(['symbol'])


# set up chart that's connected via Linker
toc = one_click(trades, by=['symbol'])

# NOTE: this only works with InputFilter, still not sure how to attach it to Linker filters
plot_toc = Figure()\
  .plot_xy(series_name="TRD", t=toc, x="ts", y="price")\
  .show()


