from deephaven import new_table, time_table, ring_table, SortDirection
from deephaven.column import int_col, float_col, string_col, double_col, datetime_col, jobj_col, Column, Sequence, InputColumn
from deephaven.stream.table_publisher import table_publisher
from deephaven.table_listener import listen

import deephaven.dtypes as dht
import json
import jpy
import numpy as np
import random as rd

from deephaven.column import InputColumn
import deephaven.dtypes as dht

def blink_tail_by(blink_table, num_rows, by):
    """ Transform blink_table to ring_table, as suggested in https://github.com/deephaven/deephaven-core/issues/4309 """
    return (
        blink_table.without_attributes(["BlinkTable"])
        .partitioned_agg_by(aggs=[], by=by, preserve_empty=True)
        .transform(lambda t: ring_table(t, num_rows))
        .merge()
    )


def generate_snapshot():
    """generate a random orderbook data snapshot as JSON string"""
    data = dict(
        symbol=rd.choice(['BTC-USD', 'ETH-USD']), 
        bids=[
            {'price': 10-rd.random(), 'qty': rd.random()*100},
            {'price': 10-rd.random(), 'qty': rd.random()*200},
            {'price': 10-rd.random(), 'qty': rd.random()*50}
        ],
        asks=[
            {'price': 11+rd.random(), 'qty': rd.random()*100},
            {'price': 11+rd.random(), 'qty': rd.random()*200},
            {'price': 11+rd.random(), 'qty': rd.random()*50}
        ],
    )
    return json.dumps(data)


my_blink_table, my_publisher = table_publisher(
    "Test table",
    {          
        "ts": dht.Instant,
        "symbol": dht.string,
        "side": dht.string,
        "price": dht.float64_array, 
        "qty": dht.float64_array,     
    }
)


def listener_function(update, is_replay):    
    rowset = update.added()  # returns a dict() with columns as arrays
    ts_array = rowset['Timestamp']  # <class 'numpy.ndarray'>
    json_array = rowset['json_str']  # <class 'numpy.ndarray'>
    
    for ts, x in zip(ts_array,json_array):
        snapshot = json.loads(x)

        bid_data = [(float(s['price']), float(s['qty'])) for s in snapshot['bids']]
        bid_prices, bid_qtys = zip(*bid_data)

        ask_data = [(float(s['price']), float(s['qty'])) for s in snapshot['asks']]
        ask_prices, ask_qtys = zip(*ask_data)

        # publish jointly one row for the bids, and one for the asks
        my_publisher.add(
            new_table(
                [
                    datetime_col("ts", [ts] * 2),
                    string_col("symbol", [snapshot['symbol']] * 2),
                    string_col("side", ['bid', 'ask']),
                    InputColumn(name='price', data_type=dht.float64_array, input_data=[bid_prices, ask_prices]),
                    InputColumn(name='qty', data_type=dht.float64_array, input_data=[bid_qtys, ask_qtys]),
                ]
            )
        )        
     
table = time_table("PT00:00:02").update("json_str=generate_snapshot()").tail(5)
handle = listen(table, listener_function)

my_ring_table = blink_tail_by(my_blink_table,1, by=['symbol', 'side'])\
    .ungroup()\
    .sort(['ts', 'symbol', 'price'], [SortDirection.ASCENDING, SortDirection.ASCENDING, SortDirection.DESCENDING])


if False:
    del table
    handle.stop()
    meta = my_blink_table.meta_table
    meta2 = my_ring_table.meta_table




