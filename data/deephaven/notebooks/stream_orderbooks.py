import deephaven.dtypes as dht
import deephaven.stream.kafka.consumer as ck
import jpy
import orjson
from deephaven import merge
from deephaven.plot import PlotStyle, Color
from deephaven.plot.figure import Figure


def create_l2_table(orderbooks_sampled):
    """Wrapper func to not clutter namespace with helper tables"""

    # ASK
    # quotes_l2_ask = orderbooks_sampled.tail(1).snapshot().select([ # use this for testing
    quotes_l2_ask = orderbooks_sampled.select([
        "ts",
        "ts_bin",
        "exchange",
        "symbol",
        "side = `ask`",
        "level = ask_matrix[0]",
        "price = ask_matrix[1]",
        "size = ask_matrix[2]",
    ]).ungroup(["level", "price", "size"])

    # BID
    # quotes_l2_bid = orderbooks_sampled.tail(1).snapshot().select([ # use this for testing
    quotes_l2_bid = (
        orderbooks_sampled.select([
            "ts",
            "ts_bin",
            "exchange",
            "symbol",
            "side = `bid`",
            "level = bid_matrix[0]",
            "price = bid_matrix[1]",
            "size = bid_matrix[2]",
        ])
        .ungroup(["level", "price", "size"])
        .update(["level = -1 * level"])
    )  # flip levels to negative for bids

    return merge([quotes_l2_bid, quotes_l2_ask]).sort(["ts_bin", "symbol", "level"])


def parse_book(book_str: str, max_levels: int = 5):
    """
    Parse bid/ask JSON string Map<price_string, size_double>, sample input looks like this:
      book_str = '{"42.91":153.69181091,"42.9":167.92181091,"42.89":202.69155819,"42.88":356.522461,"42.87":374.43279876,"42.86":1080.50800728,"42.85":165.1544641,"42.84":250,"42.83":90.91490286,"42.82":1.43268247}'

    This func casts the book_str to a Java object, i.e (3 x n_levels) matrix where values for levels, prices, sizes becomes arrays in the 1st, 2nd, and 3rd row, respectively.
    The returned Java object that can be casted to double[][] in the query language, and from there we can easily select what we need via double indicies, avoiding dealing with org.jpy.PyObject
    """
    book_dict = orjson.loads(book_str)
    vals = [
        (float(price_str), size, i + 1)
        for i, (price_str, size) in enumerate(book_dict.items())
        if i < max_levels
    ]
    j_prices, j_sizes, j_levels = [
        jpy.array("double", x) for x in zip(*vals)
    ]  # create 3 j_array of doubles
    return jpy.array(
        jpy.get_type("[D"), [j_levels, j_prices, j_sizes]
    )  # array of array


# blink table from Kafka
orderbooks_blink = ck.consume(
    {"bootstrap.servers": "redpanda:29092"},
    "orderbooks",
    key_spec=ck.KeyValueSpec.IGNORE,
    value_spec=ck.json_spec({
        "exchange": dht.string,
        "symbol": dht.string,
        "ts": dht.Instant,
        "bid": dht.string,  # json as string
        "ask": dht.string,  # json as string
    }),
    table_type=ck.TableType.blink(),
).update_view([
    "ts_bin = lowerBin(ts, 1 * SECOND)",
    "ts_cycle = (int) secondOfDay(ts, timeZone(`UTC`), false)",
])

# downsample to 1 second per symbol ('ts_cycle' ensures memory won't grow indefinitely)
# also translate raw orderbook strings to Java matrices with 1st row as levels, 2nd row as prices, and 3rd row as sizes)
orderbooks_sampled = (
    orderbooks_blink.last_by(["symbol", "ts_cycle"])
    .update([
        "ask_matrix = (double[][]) parse_book(ask)",
        "bid_matrix = (double[][]) parse_book(bid)",
    ])
    .drop_columns(["bid", "ask"])
)  # uncomment this if you wann

##############################################################################################################################
# Orderbook: Top of book (L1)
# quotes_l1 = orderbooks_sampled.tail(1).snapshot().select([ # use this for testing
quotes_l1 = orderbooks_sampled.select([
    "ts",
    "ts_bin",
    "exchange",
    "symbol",
    "bid_size = bid_matrix[2][0]",
    "ask_size = ask_matrix[2][0]",
    "bid = bid_matrix[1][0]",
    "ask = ask_matrix[1][0]",
    "mid = (bid + ask) / 2",
    "spread = ask - bid",
    "spread_bps = spread / mid * 10000",
])

# not-so-well documented trick to do as-of joins via partitions; must drop 'symbol' from one of them to avoid "conflicing columns error" for partitioned_transform() line
trades_partitioned = trades.partition_by(["symbol"])
quotes_partitioned = quotes_l1.partition_by(["symbol"]).transform(
    lambda t: t.drop_columns(["symbol", "exchange"])
)

trades_and_quotes = (
    trades_partitioned.partitioned_transform(
        quotes_partitioned, lambda t, q: t.aj(q, on=["ts"])
    )
    .merge()
    .sort(["ts"])
)

trades_and_quotes_one_symbol = trades_and_quotes.where(["symbol == `BTC-USD`"])

plot_trades_and_quotes = (
    Figure()
    .chart_title(title="Trades & Quotes")
    .plot_xy(
        series_name="TRD_BUY",
        t=trades_and_quotes_one_symbol.where("side==`buy`"),
        x="ts",
        y="price",
    )
    .point(color=Color.of_name("LAWNGREEN"))
    .plot_xy(
        series_name="TRD_SELL",
        t=trades_and_quotes_one_symbol.where("side==`sell`"),
        x="ts",
        y="price",
    )
    .point(color=Color.of_name("RED"))
    .axes(plot_style=PlotStyle.SCATTER)
    .twin()
    .plot_xy(
        series_name="BID (Kraken)", t=trades_and_quotes_one_symbol, x="ts", y="bid"
    )
    .plot_xy(
        series_name="ASK (Kraken)", t=trades_and_quotes_one_symbol, x="ts", y="ask"
    )
    .axes(plot_style=PlotStyle.STEP)
    .show()
)

plot_3subplots = (
    Figure(rows=1, cols=3)
    .new_chart(row=0, col=0)
    .chart_title(title="Trades (Coinbase vs Kraken vs Bitstamp)")
    .plot_xy(
        series_name="Coinbase",
        t=trades_and_quotes_one_symbol.where("exchange==`COINBASE`"),
        x="ts",
        y="price",
    )
    .axes(plot_style=PlotStyle.SCATTER)
    .new_chart(row=0, col=1)
    .chart_title(title="Trades (Kraken)")
    .plot_xy(
        series_name="Kraken",
        t=trades_and_quotes_one_symbol.where("exchange==`KRAKEN`"),
        x="ts",
        y="price",
    )
    .axes(plot_style=PlotStyle.SCATTER)
    .new_chart(row=0, col=2)
    .chart_title(title="Trades (Bitstamp)")
    .plot_xy(
        series_name="Bitstamp",
        t=trades_and_quotes_one_symbol.where("exchange==`BITSTAMP`"),
        x="ts",
        y="price",
    )
    .axes(plot_style=PlotStyle.SCATTER)
    .show()
)


##############################################################################################################################
from dataclasses import dataclass
from collections import OrderedDict
import numpy as np

orderbooks_curated_sampled = orderbooks_blink.last_by(["symbol", "ts_cycle"])


@dataclass
class QuoteSizeFills:
    order_sizes: list
    fill_prices: list
    best_price: float

    def get_order_sizes(self):
        return jpy.array("double", self.order_sizes)

    def get_fill_prices(self):
        return jpy.array("double", self.fill_prices)

    def get_best_price(self):
        return self.best_price


def parse_book_curated(book_str: str, max_levels: int = 200):
    order_book = json.loads(book_str)

    vals = [
        (float(price_str), size, float(price_str) * size)
        for i, (price_str, size) in enumerate(order_book.items())
        if i < max_levels
    ]
    prices, sizes, quote_sizes = [np.array(x) for x in zip(*vals)]
    cum_quote_sizes = np.insert(np.cumsum(quote_sizes), 0, 0)

    order_sizes_to_execute = [
        10e3,
        100e3,
        150e3,
        250e3,
        500e3,
        750e3,
        1e6,
        2e6,
        3e6,
    ]  # in quote quantity

    fill_indices = np.searchsorted(cum_quote_sizes, order_sizes_to_execute)
    out = OrderedDict()
    for i, fill_index in enumerate(fill_indices):
        if order_sizes_to_execute[i] <= cum_quote_sizes[-1]:
            cumulative_winsorized = np.minimum(
                cum_quote_sizes[: (fill_index + 1)], order_sizes_to_execute[i]
            )
            quote_sizes_winsorized = (
                cumulative_winsorized[1:] - cumulative_winsorized[:-1]
            )  # same as np.diff(cumulative_winsorized)
            out[order_sizes_to_execute[i]] = (
                np.dot(prices[:fill_index], quote_sizes_winsorized)
                / order_sizes_to_execute[i]
            )

    return QuoteSizeFills(list(out.keys()), list(out.values()), prices[0])


tmp = orderbooks_curated_sampled  # .tail(10).snapshot()

quotes_l2_bid_curated = (
    tmp.select([  # use this for testing
        # quotes_l2_curated = orderbooks_sampled.select([
        "ts",
        "ts_bin",
        "exchange",
        "symbol",
        "side = `bid`",
        "bid_curated = (org.jpy.PyObject) parse_book_curated(bid)",  # unclear clear why we need the explicit casting to (org.jpy.PyObject) here???
        "order_size = (double[]) bid_curated.get_order_sizes()",
        "best_price = (double) bid_curated.get_best_price()",
        "fill_price = (double[]) bid_curated.get_fill_prices()",
    ])
    .drop_columns(["bid_curated"])
    .ungroup(["order_size", "fill_price"])
    .update([
        "slippage = (fill_price/best_price - 1) * 10000",
        "order_size = -order_size",
    ])
)


quotes_l2_ask_curated = (
    tmp.select([  # use this for testing
        # quotes_l2_curated = orderbooks_sampled.select([
        "ts",
        "ts_bin",
        "exchange",
        "symbol",
        "side = `ask`",
        "ask_curated = (org.jpy.PyObject) parse_book_curated(ask)",  # unclear clear why we need the explicit casting to (org.jpy.PyObject) here???
        "order_size = (double[]) ask_curated.get_order_sizes()",
        "best_price = (double) ask_curated.get_best_price()",
        "fill_price = (double[]) ask_curated.get_fill_prices()",
    ])
    .ungroup(["order_size", "fill_price"])
    .drop_columns(["ask_curated"])
    .update([
        "slippage = (fill_price/best_price - 1) * 10000",
    ])
)

l2_book_curated = merge([quotes_l2_bid_curated, quotes_l2_ask_curated]).sort([
    "ts_bin",
    "symbol",
    "order_size",
])
l2_book_curated_one_symbol = (
    l2_book_curated.where(["symbol == `BTC-USD`"])
    .where("abs(order_size) == 100000")
    .tail_by(30, ["side"])
)

plot_l2_curated = (
    Figure()
    .axes(plot_style=PlotStyle.STEP)
    .chart_title(title="BTC-USD: Slippage for $100k")
    .plot_xy(
        series_name="100k",
        t=l2_book_curated_one_symbol,
        x="ts_bin",
        y="slippage",
        by=["side"],
    )
    .show()
)

import deephaven.plot.express as dx

fig = dx.scatter(
    l2_book_curated_one_symbol,
    x="ts_bin",
    y="slippage",
    by=["side"],
    color_discrete_sequence=["red", "lightgreen"],
    size_sequence=10,
    title="BTC-USD: Slippage for $100k",
    xaxis_titles="",
    yaxis_titles="Slippage (bps)",
)


# .plot_xy(series_name="250k", t=l2_book_curated_one_symbol.where("abs(order_size) == 2500000").tail(30), x="ts", y="slippage", by=['side']) \

# plot_l2_curated = Figure() \
#     .axes(plot_style=PlotStyle.STACKED_BAR) \
#     .chart_title(title="L2 quotes for BTC-USD") \
#     .plot_cat(series_name="BID", t=l2_book_curated_one_symbol.where(["abs(order_size) == 100000", "side == `bid`"]).tail(10), category="ts_bin", y="slippage") \
#     .plot_cat(series_name="ASK", t=l2_book_curated_one_symbol.where(["abs(order_size) == 100000", "side == `ask`"]).tail(10), category="ts_bin", y="slippage") \
#     .show()


import numpy as np
import jpy
from dataclasses import dataclass


@dataclass
class FillPrices:
    avg_fill_prices: np.array
    sizes_to_execute: np.array = np.array([1, 2, 3, 5, 20])

    def get_sizes(self) -> np.array:
        return jpy.array("double", self.sizes_to_execute)

    def get_prices(self) -> np.array:
        return jpy.array("double", self.avg_fill_prices)


def fill_price_exact(prices, sizes):
    sizes_to_execute = FillPrices.sizes_to_execute

    prices = np.array(prices)
    sizes = np.insert(np.array(sizes), 0, 0)
    cum_sizes = np.cumsum(sizes)
    fill_indices = np.searchsorted(cum_sizes, sizes_to_execute)

    avg_fill_prices = np.zeros(len(sizes_to_execute))
    for i, fill_index in enumerate(fill_indices):
        if sizes_to_execute[i] <= cum_sizes[-1]:
            cumulative_winsorized = np.minimum(
                cum_sizes[: (fill_index + 1)], sizes_to_execute[i]
            )
            sizes_winsorized = (
                cumulative_winsorized[1:] - cumulative_winsorized[:-1]
            )  # equivalent to np.diff()
            avg_fill_prices[i] = (
                np.dot(prices[:fill_index], sizes_winsorized) / sizes_to_execute[i]
            )
        else:
            break

    return FillPrices(avg_fill_prices, sizes_to_execute[: len(avg_fill_prices)])


quotes_l2_curated = (
    orderbooks_sampled.tail(3)
    .snapshot()
    .select([  # use this for testing
        # quotes_l2_curated = orderbooks_sampled.select([
        "ts",
        "ts_bin",
        "exchange",
        "symbol",
        "best_bid = bid_matrix[1][0]",
        "best_bid_size = bid_matrix[2][0]",
        "bid_prices = bid_matrix[1]",
        "bid_sizes = bid_matrix[2]",
        "bid_exact = fill_price_exact(bid_prices, bid_sizes)",  # bid_prices, bid_sizes
        "base_qty = (double []) bid_exact.get_sizes()",
        "fill_price = (double []) bid_exact.get_prices()",
    ])
)
# \.drop_columns(['bid_prices', 'bid_sizes', 'bid_exact'])\
# .ungroup(['base_qty', 'fill_price'])\
# .update('slippage = ((fill_price/best_bid) - 1) * 10000')
meta = quotes_l2_curated.meta_table


##############################################################################################################################
# Experimental (WIP): Plot L2 orderbook for BTC-USD

l2_book = create_l2_table(orderbooks_sampled)
l2_book_one_symbol = l2_book.where(["symbol ==`BTC-USD`"]).tail(1000)

plot_l2 = (
    Figure()
    .chart_title(title="L2 quotes for BTC-USD")
    .plot_xy(
        series_name="L2 book", t=l2_book_one_symbol, x="ts_bin", y="price", by=["level"]
    )
    .show()
)
