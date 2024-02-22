from deephaven import function_generated_table
from deephaven.execution_context import get_exec_ctx
import deephaven_tools as tools
import deephaven.plot.express as dx
from deephaven.plot.figure import Figure



def get_clickhouse_last100():

    symbols = ['BTC-USD']

    # # get candles from ClickHouse
    # retval = tools.get_candles(symbols, n_rows=20, freq='5 second')

    # get ticks from ClickHouse
    retval = tools.get_ticks(symbols, n_ticks=100)
    return retval



ctx = get_exec_ctx() # Get the systemic execution context
result = function_generated_table(table_generator=get_clickhouse_last100, refresh_interval_ms=1000, exec_ctx=ctx)



plot_fgt = dx.line(
    result,
    x="ts", 
    y=["price"],
    #color_discrete_sequence=["red", "lightgreen", "lightblue"],
    line_shape = 'hv',
    size_sequence=5,
    title="BTC-USD",
    xaxis_titles = '',
    yaxis_titles = 'Price',
    by=["exchange"],
)



# for comparison
# plot_fgt2 = Figure() \
#     .plot_xy(series_name="TRD2", t=result, x="ts", y="price") \
#     .show()


# orderbook query

# WITH tmp_quotes as ( 
# select 
#  toStartOfInterval(ts, INTERVAL 5 second)   AS ts_bin
#  , count(*)                                 AS cnt
#  , max(ts)                                  AS max_ts
#  , argMax(bid, ts)                          AS bid
#  , argMax(ask, ts)                          AS ask
# from cryptofeed.orderbooks
# where 
#   symbol = 'BTC-USD'
#   AND ts >=  now() - INTERVAL 20 SECOND
#   AND exchange = 'COINBASE'
# GROUP BY 1
# ORDER BY ts_bin
# )
# select 
#   *
# from tmp_quotes

