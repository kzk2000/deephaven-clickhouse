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


# latest ticks from Kafka (init as ring table to hold latest ticks in memory until the 'final_ring' is created below)
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
  table_type=ck.TableType.ring(10000))\
  .update_view([
    #'is_db = (long) 0',
    'ts_bin = lowerBin(ts, SECOND)',
  ])


def extract_book_level(x: str, level: int, field: str) -> np.float64:
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


# final ring table with utility above
quote_l1 = tools.make_ring(quotes_l1_stream, 200)





quote_l1_ring = tools.make_ring(quotes_l1_stream, 5000) #.tail_by(100, ['symbol']).sort('ts_bin')


k2 = quote_l1_ring.tail_by(2, ['ts_bin', 'symbol']).sort(['ts', 'symbol'])

lst = []
for symbol in ['BTC-USD', 'ETH-USD']:
  lst.append(tools.make_ring(quotes_l1_stream.where([f'symbol ==`{symbol}`']), 100).tail_by(3,['ts_bin']))


k2 = merge(lst)

quote_l1 = tools.make_ring(quotes_l1_stream, 5000).tail_by(2, ['ts_bin', 'symbol'])


symbolA = quote_l1.where(['symbol == `BTC-USD`'])
