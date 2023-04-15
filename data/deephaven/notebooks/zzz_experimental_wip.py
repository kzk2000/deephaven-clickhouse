import deephaven_tools as tools


# 10 rows per ticker using partitioned tables
partitioned_table, keys_list = tools.get_partitions(final_stream, 'symbol')

lst = []
for index, key in enumerate(keys_list):
    table_now = partitioned_table.get_constituent([key])
    lst.append(table_now.tail(3))

tail5 = Table(JRingTableTools.of(merge(lst).j_table, 12))\
  .last_by(['symbol', 'exchange', 'trade_id'])\
  .drop_columns(['KafkaOffset'])\
  .sort(['ts'])

kk = final_stream.tail_by(1, by=['symbol'])

# tail5 = final_ring.tail_by(300, by=['symbol'])
# last_by_symbol = tail5.last_by('symbol')



#######################################################
# load data from CLickHouse into static tables
from importlib import reload
reload(tools)

symbols = ['BTC-USD']

# get ticks from ClickHouse
ticks_static = tools.get_ticks(symbols, n_ticks=100)

# get candles from ClickHouse
candles_static = tools.get_candles(symbols)


