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
first_ts = trades_kafka.agg_by([agg.min_('ts')])
first_ts_py = dhpd.to_pandas(first_ts).iloc[0, 0]

query_history = f"""
  SELECT *, toInt64(1) as is_db FROM cryptofeed.trades
  WHERE 
    ts >= now() - INTERVAL 3 HOUR
    AND ts <= '{first_ts_py}'
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


# FIXME: this doesn't apply the filter from the linker -- check with DH eng
# plot_trades_one_symbol = Figure()\
#   .plot_xy(series_name="TRD", t=trades, x="ts", y="price")\
#   .show()

# trades_n_quotes = trades_ring.aj(table=quotes_l1_ring , on=["symbol", "ts"])
# trades_n_quotesA = trades_n_quotes.where(['symbol == `BTC-USD`']).tail(300)

# plot_bid_vs_ask = Figure()\
#   .chart_title(title="Trades & Quotes")\
#   .plot_xy(series_name="TRD_BUY", t=trades_n_quotesA.where('side==`buy`'), x="ts", y="price")\
#   .plot_xy(series_name="TRD_SELL", t=trades_n_quotesA.where('side==`sell`'), x="ts", y="price")\
#   .axes(plot_style=PlotStyle.SCATTER)\
#   .twin()\
#   .plot_xy(series_name="BID", t=trades_n_quotesA, x="ts", y="bid")\
#   .plot_xy(series_name="ASK", t=trades_n_quotesA, x="ts", y="ask")\
#   .axes(plot_style=PlotStyle.STEP)\
#   .show()
