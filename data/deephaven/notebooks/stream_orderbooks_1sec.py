import clickhouse_connect
import jpy
import time
import json
import numpy as np
import deephaven.dtypes as dht
import deephaven.pandas as dhpd
from deephaven.plot.figure import Figure
from deephaven.plot import LineEndStyle, LineJoinStyle, LineStyle, Colors
import deephaven.stream.kafka.consumer as ck
from deephaven import agg, merge, ring_table
from deephaven.table import Table
import deephaven
import deephaven_tools as tools



orderbooks_1sec = ck.consume(
  {'bootstrap.servers': 'redpanda:29092'},
  'orderbooks_1sec',
  key_spec=ck.KeyValueSpec.IGNORE,
  value_spec=ck.json_spec([
    ('exchange', dht.string),
    ('symbol', dht.string),
    ('ts_latest', dht.Instant),
    ('ts_1sec', dht.Instant),
    ('bid', dht.string), 
    ('ask', dht.string), 
  ]),
  table_type=ck.TableType.blink()
)


def extract_book_level(x: str, level: int, field: str) -> np.float64:
    """Orderbook JSON string -> dict() -> float of price/size at level"""
    if field == 'price':
        return np.float64(list(json.loads(x).keys())[level])
    elif field == 'size':
        return list(json.loads(x).values())[level]


quotes_1sec_l1_stream = orderbooks_1sec.view([
  'ts_latest',
  'ts_1sec',
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
quotes_l1_ring = ring_table(quotes_1sec_l1_stream, 20000).tail_by(5000, ['symbol'])

quotes_per_second = orderbooks_1sec\
  .agg_by([agg.count_('count')], by=["ts_1sec"])\
  .tail(10)

quotes_one_symbol = quotes_l1_ring.where(['symbol == `BTC-USD`'])


plot_bid_vs_ask = Figure()\
  .plot_xy(series_name="BID", t=quotes_one_symbol, x="ts_1sec", y="bid")\
  .plot_xy(series_name="ASK", t=quotes_one_symbol, x="ts_1sec", y="ask")\
  .show()

#plot bid-ask spread
plot_spread_bps = Figure()\
  .plot_xy(series_name="SPREAD_BPS", t=quotes_one_symbol, x="ts_1sec", y="spread_bps")\
  .show()



plot_bid_vs_ask = Figure()\
  .chart_title(title="Trades & Quotes")\
  .plot_xy(series_name="TRD_BUY", t=trades_n_quotesA.where('side==`buy`'), x="ts", y="price")\
  .plot_xy(series_name="TRD_SELL", t=trades_n_quotesA.where('side==`sell`'), x="ts", y="price")\
  .axes(plot_style=PlotStyle.SCATTER)\
  .twin()\
  .plot_xy(series_name="BID", t=trades_n_quotesA, x="ts", y="bid")\
  .plot_xy(series_name="ASK", t=trades_n_quotesA, x="ts", y="ask")\
  .axes(plot_style=PlotStyle.STEP)\
  .show()
 

# 
f = Figure(rows=1, cols=2)\
    .new_chart(row=0, col=0).plot_xy(series_name="SPREAD", t=quotes_one_symbol, x="ts", y="spread")\
    .new_chart(row=0, col=1).plot_xy(series_name="SPREAD_BPS", t=quotes_one_symbol, x="ts", y="spread_bps")\
    .show()
