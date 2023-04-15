import clickhouse_connect
import jpy
import time
import json
import numpy as np
import deephaven.dtypes as dht
import deephaven.pandas as dhpd
import deephaven.stream.kafka.consumer as ck
from deephaven import agg, merge
from deephaven.table import Table
import deephaven
import deephaven_tools as tools

# create a small
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
  table_type=ck.TableType.ring(1000))\
  .update_view([
    'ts_bin = lowerBin(ts, SECOND)',
  ]).last_by(['ts_bin', 'symbol'])

# python wrapper to exract fields from JSON
def extract_book_level(x: str, level: int, field: str) -> np.float64:
  if field == 'price':
    return np.float64(list(json.loads(x).keys())[level])
  elif field == 'size':
    return np.float64(list(json.loads(x).values())[level])
    
# extract fields from JSON string with the Python wrapper 'extract_book_level'
quotes_l1_stream = orderbooks.view([
  'ts',
  'ts_bin',
  'symbol',
  'bid_size = extract_book_level(bid, 0, `size`)',
  'ask_size = extract_book_level(ask, 0, `size`)',
 # 'bid = extract_book_level(bid, 0, `price`)',
  #'ask = extract_book_level(ask, 0, `price`)',
  #'mid = (bid + ask) / 2',
 # 'spread = ask - bid',
  #'spread_bps = spread / mid * 10000',
])

# final ring table with utility above
quote_l1d = tools.make_ring(quotes_l1_stream, 20)

