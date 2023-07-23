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
from deephaven.time import second_of_day, time_zone, minute_of_day
import deephaven

import deephaven_tools as tools


# blink table from Kafka
orderbooks = ck.consume(
  {'bootstrap.servers': 'redpanda:29092'},
  'orderbooks',
  key_spec=ck.KeyValueSpec.IGNORE,
  value_spec=ck.json_spec([
    ('exchange', dht.string),
    ('symbol', dht.string),
    ('ts', dht.Instant),
    ('bid', dht.string),  # json as string
    ('ask', dht.string),  # json as string
  ]),
  table_type=ck.TableType.blink())\
  .update_view([
    'ts_bin = lowerBin(ts, 1 * SECOND)',
    'ts_cycle = (int) second_of_day(ts, time_zone(`ET`))',
  ])

# downsample to 1 second intervals, 'ts_cycle' ensures memory growth to be finite (using ts_bin would blow up memory eventually)
order_books_sampled = orderbooks.last_by(['symbol', 'ts_cycle']).sort(['ts_bin'])


def extract_book_level(x: str, level: int, field: str) -> np.float64:
    """Orderbook JSON string -> dict() -> float of price/size at level"""
    if field == 'price':
        return np.float64(list(json.loads(x).keys())[level])
    elif field == 'size':
        return list(json.loads(x).values())[level]


quotes_l1 = order_books_sampled.select([
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

# create ring table -> this throws...need to file bug report
# quotes_l1_ring = ring_table(quotes_l1, 40)

quotes_per_second = orderbooks\
  .agg_by([agg.last('ts_bin'), agg.count_('count')], by=["ts_cycle"])\
  .tail(10)

quotes_one_symbol = quotes_l1.where(['symbol == `BTC-USD`'])

plot_bid_vs_ask = Figure()\
  .plot_xy(series_name="BID", t=quotes_one_symbol, x="ts", y="bid")\
  .plot_xy(series_name="ASK", t=quotes_one_symbol, x="ts", y="ask")\
  .show()

# plot bid-ask spread
plot_spread_bps = Figure()\
  .plot_xy(series_name="SPREAD_BPS", t=quotes_one_symbol, x="ts", y="spread_bps")\
  .show()


# Note: quote_l1 is subsampled here
trades_and_quotes = trades.aj(table=quotes_l1, on=["symbol", "ts"])
trades_and_quotes_one_symbol = trades_and_quotes.where(['symbol == `BTC-USD`']).tail(300)

plot_bid_vs_ask = Figure()\
  .chart_title(title="Trades & Quotes")\
  .plot_xy(series_name="TRD_BUY", t=trades_and_quotes_one_symbol.where('side==`buy`'), x="ts", y="price")\
  .plot_xy(series_name="TRD_SELL", t=trades_and_quotes_one_symbol.where('side==`sell`'), x="ts", y="price")\
  .axes(plot_style=PlotStyle.SCATTER)\
  .twin()\
  .plot_xy(series_name="BID", t=trades_and_quotes_one_symbol, x="ts", y="bid")\
  .plot_xy(series_name="ASK", t=trades_and_quotes_one_symbol, x="ts", y="ask")\
  .axes(plot_style=PlotStyle.STEP)\
  .show()
 

plot_2subplots = Figure(rows=1, cols=2)\
    .new_chart(row=0, col=0).plot_xy(series_name="SPREAD", t=quotes_one_symbol, x="ts", y="spread")\
    .new_chart(row=0, col=1).plot_xy(series_name="SPREAD_BPS", t=quotes_one_symbol, x="ts", y="spread_bps")\
    .show()



