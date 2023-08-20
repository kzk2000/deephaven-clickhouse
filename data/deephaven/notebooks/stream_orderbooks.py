import clickhouse_connect
import jpy
import time
import json
import numpy as np
import deephaven
import deephaven.dtypes as dht
import deephaven.pandas as dhpd
import deephaven.stream.kafka.consumer as ck
from deephaven import agg, merge, ring_table
from deephaven.plot import LineEndStyle, LineJoinStyle, LineStyle, Colors, PlotStyle, Color
from deephaven.plot.figure import Figure
from deephaven.table import Table
from deephaven.time import second_of_day, time_zone, minute_of_day


import deephaven_tools as tools


# blink table from Kafka
orderbooks_blink = ck.consume(
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
  table_type=ck.TableType.blink()
) \
.update_view([
  'ts_bin = lowerBin(ts, 1 * SECOND)',
  'ts_cycle = (int) secondOfDay(ts, timeZone(`UTC`))',
])

# downsample to 1 second per symbol ('ts_cycle' ensures memory growth to be finite)
orderbooks_sampled = orderbooks_blink.last_by(['symbol', 'ts_cycle'])

def extract_book_level(x: str, level: int, field: str) -> np.float64:
    """Orderbook JSON string -> dict() -> float of price/size at level"""
    if field == 'price':
        return np.float64(list(json.loads(x).keys())[level])
    elif field == 'size':
        return list(json.loads(x).values())[level]


quotes_l1 = orderbooks_sampled.select([
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

# not-so-well documented trick by DH Engs to do as-of joins via partitions; must drop 'symbol' from one of them to avoid "conflicing columns error" for partitioned_transform() line
trades_partitioned = trades.partition_by(['symbol'])
quotes_partitioned = quotes_l1.partition_by(['symbol']).transform(lambda t: t.drop_columns(['symbol']))  

trades_and_quotes = trades_partitioned.partitioned_transform(quotes_partitioned, lambda t, q: t.aj(q, on=['ts'])).merge().sort(['ts'])


trades_and_quotes_one_symbol = trades_and_quotes.where(['symbol == `BTC-USD`'])

plot_trades_and_quotes = Figure()\
  .chart_title(title="Trades & Quotes")\
  .plot_xy(series_name="TRD_BUY", t=trades_and_quotes_one_symbol.where('side==`buy`'), x="ts", y="price")\
  .point(color=Color.of_name("LAWNGREEN"))\
  .plot_xy(series_name="TRD_SELL", t=trades_and_quotes_one_symbol.where('side==`sell`'), x="ts", y="price")\
  .point(color=Color.of_name("RED"))\
  .axes(plot_style=PlotStyle.SCATTER)\
  .twin()\
  .plot_xy(series_name="BID (Kraken)", t=trades_and_quotes_one_symbol, x="ts", y="bid")\
  .plot_xy(series_name="ASK (Kraken)", t=trades_and_quotes_one_symbol, x="ts", y="ask")\
  .axes(plot_style=PlotStyle.STEP)\
  .show()
 

plot_3subplots = Figure(rows=1, cols=3)\
    .new_chart(row=0, col=0)\
    .chart_title(title="Trades (Coinbase vs Kraken vs Bitstamp)")\
    .plot_xy(series_name="Coinbase", t=trades_and_quotes_one_symbol.where("exchange==`COINBASE`"), x="ts", y="price")\
    .axes(plot_style=PlotStyle.SCATTER)\
    .new_chart(row=0, col=1)\
    .chart_title(title="Trades (Kraken)")\
    .plot_xy(series_name="Kraken", t=trades_and_quotes_one_symbol.where("exchange==`KRAKEN`"), x="ts", y="price")\
    .axes(plot_style=PlotStyle.SCATTER)\
    .new_chart(row=0, col=2)\
    .chart_title(title="Trades (Bitstamp)")\
    .plot_xy(series_name="Bitstamp", t=trades_and_quotes_one_symbol.where("exchange==`BITSTAMP`"), x="ts", y="price")\
    .axes(plot_style=PlotStyle.SCATTER)\
    .show()



