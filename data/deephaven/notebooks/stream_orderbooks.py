import deephaven.dtypes as dht
import deephaven.stream.kafka.consumer as ck
import jpy
import json
from deephaven import merge
from deephaven.plot import PlotStyle, Color
from deephaven.plot.figure import Figure


def create_l2_table(orderbooks_sampled):
    """Wrapper func to not clutter namespace with helper tables"""

    # ASK
    # quotes_l2_ask = orderbooks_sampled.tail(1).snapshot().select([ # use this for testing
    quotes_l2_ask = orderbooks_sampled.select([
        'ts',
        'ts_bin',
        'exchange',
        'symbol',
        'side = `ask`',
        'level = ask_matrix[0]',
        'price = ask_matrix[1]',
        'size = ask_matrix[2]',
    ]).ungroup(['level', 'price', 'size'])

    # BID
    # quotes_l2_bid = orderbooks_sampled.tail(1).snapshot().select([ # use this for testing
    quotes_l2_bid = orderbooks_sampled.select([
        'ts',
        'ts_bin',
        'exchange',
        'symbol',
        'side = `bid`',
        'level = bid_matrix[0]',
        'price = bid_matrix[1]',
        'size = bid_matrix[2]',
    ]).ungroup(['level', 'price', 'size']) \
        .update(["level = -1 * level"])  # flip levels to negative for bids

    return merge([quotes_l2_bid, quotes_l2_ask]).sort(['ts_bin', 'symbol', 'level'])


def parse_book(book_str: str, max_levels: int = 5):
    """
    Parse bid/ask JSON string Map<price_string, size_double>, sample input looks like this:
      book_str = '{"42.91":153.69181091,"42.9":167.92181091,"42.89":202.69155819,"42.88":356.522461,"42.87":374.43279876,"42.86":1080.50800728,"42.85":165.1544641,"42.84":250,"42.83":90.91490286,"42.82":1.43268247}'

    This func casts the book_str to a Java object, i.e (3 x n_levels) matrix where values for levels, prices, sizes becomes arrays in the 1st, 2nd, and 3rd row, respectively.
    The returned Java object that can be casted to double[][] in the query language, and from there we can easily select what we need via double indicies, avoiding dealing with org.jpy.PyObject
    """
    book_dict = json.loads(book_str)
    vals = [(float(price_str), size, i + 1) for i, (price_str, size) in enumerate(book_dict.items()) if i < max_levels]
    j_prices, j_sizes, j_levels = [jpy.array("double", x) for x in zip(*vals)]  # create 3 j_array of doubles
    return jpy.array(jpy.get_type('[D'), [j_levels, j_prices, j_sizes])  # array of array


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
).update_view([
    'ts_bin = lowerBin(ts, 1 * SECOND)',
    'ts_cycle = (int) secondOfDay(ts, timeZone(`UTC`))',
])

# downsample to 1 second per symbol ('ts_cycle' ensures memory won't grow indefinitely)
# also translate raw orderbook strings to Java matrices with 1st row as levels, 2nd row as prices, and 3rd row as sizes)
orderbooks_sampled = orderbooks_blink.last_by(['symbol', 'ts_cycle']).update([
    'ask_matrix = (double[][]) parse_book(ask)',
    'bid_matrix = (double[][]) parse_book(bid)',
])

##############################################################################################################################
# Orderbook: Top of book (L1)
# quotes_l1 = orderbooks_sampled.tail(1).snapshot().select([ # use this for testing
quotes_l1 = orderbooks_sampled.select([
    'ts',
    'ts_bin',
    'exchange',
    'symbol',
    'bid_size = bid_matrix[2][0]',
    'ask_size = ask_matrix[2][0]',
    'bid = bid_matrix[1][0]',
    'ask = ask_matrix[1][0]',
    'mid = (bid + ask) / 2',
    'spread = ask - bid',
    'spread_bps = spread / mid * 10000',
])

# not-so-well documented trick to do as-of joins via partitions; must drop 'symbol' from one of them to avoid "conflicing columns error" for partitioned_transform() line
trades_partitioned = trades.partition_by(['symbol'])
quotes_partitioned = quotes_l1.partition_by(['symbol']).transform(lambda t: t.drop_columns(['symbol', 'exchange']))

trades_and_quotes = trades_partitioned.partitioned_transform(quotes_partitioned, lambda t, q: t.aj(q, on=['ts'])).merge().sort(['ts'])

trades_and_quotes_one_symbol = trades_and_quotes.where(['symbol == `BTC-USD`'])

plot_trades_and_quotes = Figure() \
    .chart_title(title="Trades & Quotes") \
    .plot_xy(series_name="TRD_BUY", t=trades_and_quotes_one_symbol.where('side==`buy`'), x="ts", y="price") \
    .point(color=Color.of_name("LAWNGREEN")) \
    .plot_xy(series_name="TRD_SELL", t=trades_and_quotes_one_symbol.where('side==`sell`'), x="ts", y="price") \
    .point(color=Color.of_name("RED")) \
    .axes(plot_style=PlotStyle.SCATTER) \
    .twin() \
    .plot_xy(series_name="BID (Kraken)", t=trades_and_quotes_one_symbol, x="ts", y="bid") \
    .plot_xy(series_name="ASK (Kraken)", t=trades_and_quotes_one_symbol, x="ts", y="ask") \
    .axes(plot_style=PlotStyle.STEP) \
    .show()

plot_3subplots = Figure(rows=1, cols=3) \
    .new_chart(row=0, col=0) \
    .chart_title(title="Trades (Coinbase vs Kraken vs Bitstamp)") \
    .plot_xy(series_name="Coinbase", t=trades_and_quotes_one_symbol.where("exchange==`COINBASE`"), x="ts", y="price") \
    .axes(plot_style=PlotStyle.SCATTER) \
    .new_chart(row=0, col=1) \
    .chart_title(title="Trades (Kraken)") \
    .plot_xy(series_name="Kraken", t=trades_and_quotes_one_symbol.where("exchange==`KRAKEN`"), x="ts", y="price") \
    .axes(plot_style=PlotStyle.SCATTER) \
    .new_chart(row=0, col=2) \
    .chart_title(title="Trades (Bitstamp)") \
    .plot_xy(series_name="Bitstamp", t=trades_and_quotes_one_symbol.where("exchange==`BITSTAMP`"), x="ts", y="price") \
    .axes(plot_style=PlotStyle.SCATTER) \
    .show()

##############################################################################################################################
# Experimental (WIP): Plot L2 orderbook for BTC-USD

l2_book = create_l2_table(orderbooks_sampled)
l2_book_one_symbol = l2_book.where(["symbol ==`BTC-USD`"]).tail(1000)

plot_l2 = Figure() \
    .chart_title(title="L2 quotes for BTC-USD") \
    .plot_xy(series_name="L2 book", t=l2_book_one_symbol, x="ts_bin", y="price", by=['level']) \
    .show()
