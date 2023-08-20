import deephaven_tools as tools
import pandas as pd
import numpy as np
from importlib import reload
reload(tools)

#######################################################
# load data from CLickHouse into static tables

symbols = ['BTC-USD']

# get candles from ClickHouse
candles_static = tools.get_candles(symbols, n_rows=20, freq='15 minute')

# get ticks from ClickHouse
ticks_static = tools.get_ticks(symbols, n_ticks=100)



