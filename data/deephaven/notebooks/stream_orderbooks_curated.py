import orjson # faster than regular json
from collections import OrderedDict
from dataclasses import dataclass, field

import deephaven.dtypes as dht
import deephaven.plot.express as dx
import deephaven.stream.kafka.consumer as ck
import jpy
import numpy as np


@dataclass
class CuratedOrderSizes:
    order_sizes: list
    fill_prices: list
    best_price: float
    dictionary: dict = field(init=False)

    def __post_init__(self):
        self.dictionary = {k: v for (k, v) in zip(self.order_sizes, self.fill_prices)}

    def get_order_sizes(self):  # FIXME: type hints don't seem to matter for in Class methods
        return jpy.array("double", self.order_sizes)

    def get_fill_prices(self):
        return jpy.array("double", self.fill_prices)

    def get_best_price(self):
        return self.best_price

    def get_fill_price_for_qty(self, qty) -> float:
        return self.dictionary.get(qty, np.nan)


def parse_book_curated(book_str: str, max_levels: int = 200):
    """
    Parse bid/ask JSON string Map<price_string, size_double>, sample input 'book_str' looks like this:
      book_str = '{"42.91":153.69181091,"42.9":167.92181091,"42.89":202.69155819,"42.88":356.522461,"42.87":374.43279876,"42.86":1080.50800728,"42.85":165.1544641,"42.84":250,"42.83":90.91490286,"42.82":1.43268247}'

    and compute EXACT fill prices for order_sizes_to_execute = [1e3, 10e3, 100e3, 250e3] 
    """

    order_book = orjson.loads(book_str)

    vals = [(float(price_str), size, float(price_str) * size) for i, (price_str, size) in enumerate(order_book.items())]
    prices, sizes, quote_sizes = [np.array(x) for x in zip(*vals)]
    cum_quote_sizes = np.insert(np.cumsum(quote_sizes), 0, 0)  # add leading [0] here, so we can use np.diff() equivalent logic below

    order_sizes_to_execute = [1e3, 10e3, 100e3, 250e3]  # in quote quantity, usually USD

    fill_indices = np.searchsorted(cum_quote_sizes, order_sizes_to_execute)
    out = OrderedDict()
    for i, fill_index in enumerate(fill_indices):
        if order_sizes_to_execute[i] <= cum_quote_sizes[-1]:
            cumulative_winsorized = np.minimum(cum_quote_sizes[:(fill_index + 1)], order_sizes_to_execute[i])
            quote_sizes_winsorized = cumulative_winsorized[1:] - cumulative_winsorized[:-1]  # same as np.diff(cumulative_winsorized)
            out[order_sizes_to_execute[i]] = np.dot(prices[:fill_index], quote_sizes_winsorized) / order_sizes_to_execute[i]

    return CuratedOrderSizes(list(out.keys()), list(out.values()), prices[0])


def convert_sampled_to_curated(orderbooks_sampled):
    """
    Takes 1-second snapshots and does all the data massaging to get the final table
    
    Note: This makes heavy use of Python-Java bridge -- yet to be seen if this won't blow up memory
    
    """

    # orderbooks_sampled = orderbooks_sampled.tail(10).snapshot()  # useful for prototyping - DON'T FORGET to comment when done!

    # BID
    bid_curated = orderbooks_sampled.select([
        'ts_bin',
        'exchange',
        'symbol',
        'bid_curated = (org.jpy.PyObject) parse_book_curated(bid)',    # unclear why an explicit casting to (org.jpy.PyObject) is required here???
        'best_bid = (double) bid_curated.get_best_price()', # replaced input 'bid' JSON string with best_bid
        'bid_1k = (double) bid_curated.get_fill_price_for_qty(1000)',
        'bid_10k = (double) bid_curated.get_fill_price_for_qty(10000)',
        'bid_100k = (double) bid_curated.get_fill_price_for_qty(100000)',
        'bid_250k = (double) bid_curated.get_fill_price_for_qty(250000)',
        'bid_slippage_1k = (bid_1k/best_bid - 1) * 10000',
        'bid_slippage_10k = (bid_10k/best_bid - 1) * 10000',
        'bid_slippage_100k = (bid_100k/best_bid - 1) * 10000',
        'bid_slippage_250k = (bid_250k/best_bid - 1) * 10000',
    ]).drop_columns(['bid_curated'])

    # ASK
    ask_curated = orderbooks_sampled.select([     
        'ts_bin',
        'exchange',
        'symbol',    
        'ask_curated = (org.jpy.PyObject) parse_book_curated(ask)',    # unclear why an explicit casting to (org.jpy.PyObject) is required here???
        'best_ask = (double) ask_curated.get_best_price()',
        'ask_1k = (double) ask_curated.get_fill_price_for_qty(1000)',
        'ask_10k = (double) ask_curated.get_fill_price_for_qty(10000)',
        'ask_100k = (double) ask_curated.get_fill_price_for_qty(100000)',
        'ask_250k = (double) ask_curated.get_fill_price_for_qty(250000)',
        'ask_slippage_1k = (ask_1k/best_ask - 1) * 10000',
        'ask_slippage_10k = (ask_10k/best_ask - 1) * 10000',
        'ask_slippage_100k = (ask_100k/best_ask - 1) * 10000',
        'ask_slippage_250k = (ask_250k/best_ask - 1) * 10000',
    ]).drop_columns(['ask_curated'])

    # join to final table
    l2_curated = bid_curated.natural_join(ask_curated, on=['ts_bin', 'exchange', 'symbol'])

    # also add round-trip costs (in basis points) for different order sizes
    l2_curated = l2_curated.update([
        'mid = (best_bid + best_ask) / 2',
        'spread_bps = (best_ask - best_bid ) / mid * 10000',
        'spread_1k_bps = (ask_1k - bid_1k) / mid * 10000',
        'spread_10k_bps = (ask_10k - bid_10k) / mid * 10000',
        'spread_100k_bps = (ask_100k - bid_100k) / mid * 10000',
        'spread_250k_bps = (ask_250k - bid_250k) / mid * 10000',
    ]) 

    # final re-ordering
    l2_curated = l2_curated.move_columns_up(['ts_bin', 'exchange', 'symbol', 'best_bid', 'best_ask', 'mid' ])

    return l2_curated 


def get_orderbooks_curated():
    """Wrapper to get Blink table from Kafka with raw orderbook snapshots as JSON strings, and then convert that to 1-second curated snapshots"""
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
    ).update_view([
        'ts_bin = lowerBin(ts, 1 * SECOND)',
        'ts_cycle = (int) secondOfDay(ts, timeZone(`UTC`))',
    ])

    orderbooks_sampled = orderbooks_blink.last_by(['exchange', 'symbol', 'ts_cycle']).move_columns_down(['bid', 'ask'])
    orderbooks_curated = convert_sampled_to_curated(orderbooks_sampled).sort(['ts_bin'])
    return orderbooks_curated



##############################################################################################################################
# start subscription and parsing

orderbooks_curated = get_orderbooks_curated()


##############################################################################################################################
# plot a few things

orderbook_one_symbol = orderbooks_curated.where(["symbol == `BTC-USD`"]).tail(100).sort(['ts_bin'])

slippage_100k = dx.line(
    orderbook_one_symbol,
    x="ts_bin", 
    y=["bid_slippage_100k", "ask_slippage_100k"],
    color_discrete_sequence=["red", "lightgreen", "lightblue"],
   # color_discrete_map = dict(COINBASE= "green"),
    line_shape = 'hv',
    size_sequence=8,
    title="BTC-USD: Slippage for $100k",
    xaxis_titles = '',
    yaxis_titles = 'Slippage (bps)',
    by=["exchange", "variable"], # 'variable' dimension is a plotly oddity, see explanation at https://github.com/deephaven/deephaven-plugins/issues/209
)


spreads_100k = dx.line(
    orderbook_one_symbol, 
    x="ts_bin", 
    y=["spread_100k_bps"],
    #color_discrete_sequence=["red", "lightgreen", "lightblue"],
    color_discrete_map = {("COINBASE", "BTC-USD"): "green"},
    line_shape = 'hv',
    size_sequence=8,
    title="Round-trip cost for $100k",
    xaxis_titles = '',
    yaxis_titles = 'Slippage (bps)',  
    by=['exchange', 'symbol'],
)


prices = dx.line(
    orderbook_one_symbol, 
    x="ts_bin", 
    y=["best_ask"],
    color_discrete_sequence=["red", "lightgreen", "lightblue"],
    line_shape = 'hv',
    size_sequence=8,
    title="Mid-price by Exchange",
    xaxis_titles = '',
    yaxis_titles = 'Slippage (bps)',  
    by=['exchange', 'symbol'],
)


fig22 = dx.make_subplots(slippage_100k,spreads_100k, rows=1,cols=2)




