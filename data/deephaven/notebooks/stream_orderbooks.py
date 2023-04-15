import clickhouse_connect
import jpy
import time
import json
import numpy as np
import deephaven.dtypes as dht
import deephaven.pandas as dhpd
from deephaven.plot.figure import Figure
import deephaven.stream.kafka.consumer as ck
from deephaven import agg, merge
from deephaven.table import Table
import deephaven
import deephaven_tools as tools



# stream table from Kafka
orderbooks = ck.consume(
  {'bootstrap.servers': 'redpanda:29092'},
  'orderbooks',
  key_spec=ck.KeyValueSpec.IGNORE,
  value_spec=ck.json_spec([
    ('exchange', dht.string),
    ('symbol', dht.string),
    ('ts', dht.DateTime),
    ('bid', dht.string),  # json as string
    ('ask', dht.string),  # json as string
  ]),
  table_type=ck.TableType.stream())\
  .update_view([
    #'is_db = (long) 0',
    'ts_bin = lowerBin(ts, SECOND)',
  ])


def extract_book_level(x: str, level: int, field: str) -> np.float64:
    """Orderbook JSON string -> dict() -> float of price/size at level"""
    if field == 'price':
        return np.float64(list(json.loads(x).keys())[level])
    elif field == 'size':
        return list(json.loads(x).values())[level]


quotes_l1_stream = orderbooks.view([
  'ts',
  'ts_bin',
  'symbol',
  'bid_size = extract_book_level(bid, 0, `size`)',
  'ask_size = extract_book_level(ask, 0, `size`)',
  'bid = extract_book_level(bid, 0, `price`)',
  'ask = extract_book_level(ask, 0, `price`)',
  'mid = (bid + ask) / 2',
  'spread = ask - bid',
  'spread_bps = spread / mid * 10000',
])

# create ring table
quotes_l1_ring = tools.make_ring(quotes_l1_stream, 200000).tail_by(50000, ['symbol', 'ts_bin'])

quotes_per_second = orderbooks\
  .agg_by([agg.count_('count')], by=["ts_bin"])\
  .tail(10)



# some plots
quotes_one_symbol = quotes_l1_ring.where(['symbol == `BTC-USD`'])

plot_bid_vs_ask = Figure()\
  .plot_xy(series_name="BID", t=quotes_one_symbol, x="ts", y="bid")\
  .plot_xy(series_name="ASK", t=quotes_one_symbol, x="ts", y="ask")\
  .show()

#plot bid-ask spread
plot_spread_bps = Figure()\
  .plot_xy(series_name="SPREAD_BPS", t=quotes_one_symbol, x="ts", y="spread_bps")\
  .show()

 
# from deephaven.plot import LineEndStyle, LineJoinStyle, LineStyle, Colors

# figure = Figure()
# plot_step = figure\
#     .axes(plot_style=PlotStyle.STEP)\
#     .plot_xy(series_name="HeartRate", t=source, x="Time", y="HeartRate")\
#     .line(style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))\
#     .show()

# f = Figure(rows=1, cols=2)\
#     .new_chart(row=0, col=0).plot_xy(series_name="SPREAD", t=quotes_one_symbol, x="ts", y="spread")\
#     .new_chart(row=0, col=1).plot_xy(series_name="SPREAD_BPS", t=quotes_one_symbol, x="ts", y="spread_bps")\
#     .show()
