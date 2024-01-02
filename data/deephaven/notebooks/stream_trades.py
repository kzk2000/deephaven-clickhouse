import deephaven.dtypes as dht
import deephaven.stream.kafka.consumer as ck
from deephaven import agg, merge
from deephaven.plot.figure import Figure
from deephaven.plot.selectable_dataset import one_click

import deephaven_tools as tools

# latest from Kafka (blink table)
trades_blink = ck.consume(
    {'bootstrap.servers': 'redpanda:29092'},
    'trades',
    key_spec=ck.KeyValueSpec.IGNORE,
    value_spec=ck.json_spec([
        ('ts', dht.Instant),
        ('receipt_ts', dht.Instant),
        ('symbol', dht.string),
        ('exchange', dht.string),
        ('side', dht.string),
        ('size', dht.double),
        ('price', dht.double),
        ('trade_id', dht.int_),
    ]),
    table_type=ck.TableType.blink()
)\
.drop_columns(['KafkaPartition', 'KafkaTimestamp'])\
.update_view('is_db = (long) 0')

trades_kafka = tools.blink_tail_by(trades_blink, 5000, by=['symbol'])
trades_kafka.j_table.awaitUpdate()  # this waits for at least 1 tick before we continue below this line

# historical ticks from ClickHouse
query_history = f"""
  SELECT *, toInt64(1) as is_db FROM cryptofeed.trades
  WHERE
    ts >= now() - INTERVAL 10 MINUTE
    AND ts < '{tools.get_first_ts(trades_kafka)}'
  ORDER BY ts ASC
"""
trades_clickhouse = tools.query_clickhouse(query_history)

trades = merge([trades_clickhouse, trades_kafka]).drop_columns(['KafkaOffset', 'receipt_ts'])

# some summay stats
tick_count_by_exch = trades.agg_by(agg.count_('count'), by=['symbol', 'exchange'])
last_trade = trades.last_by(['symbol']).sort(['symbol'])

# FIXME: uncomment this for now to not block startup
# set up a chart that's connected via Linker
# toc = one_click(trades, by=['symbol'])

# plot_toc = Figure() \
#     .plot_xy(series_name="TRD", t=toc, x="ts", y="price") \
#     .show()


