import pandas as pd
import pyarrow.parquet as pq
import deephaven.arrow as dhaw
import deephaven.pandas as dhpd
import deephaven.parquet as dhpq


test_table = trades.tail(5).snapshot()

root_folder = '/data/parquet/testing/'
arrow_table = dhaw.to_arrow(test_table)

# write a partioned parquet files
pq.write_to_dataset(
    arrow_table,
    root_folder,
    partition_cols=['symbol'],
    existing_data_behavior='delete_matching',
    version="2.6")   # version="2.6" is required to save nanosecond timestamps

dh_table = dhpq.read(root_folder, is_refreshing=True)  # 

btc_only = dh_table.where(['symbol==`BTC-USD`'])














# arrow_data = dhaw.to_arrow(test_table)

# file_name = '/data/parquet/test_table.parquet'
# dhpq.write(test_table, file_name)


# dhpd.pandas.to

# k2 = dhpq.read(file_name)

# parquet_root = '/data/parquet/testing/'

# mt = test_table.meta_table

# dhpd.to_pandas(test_table.drop_columns(['receipt_ts', 'ts'])).to_parquet(
#     path=parquet_root, 
#     partition_cols = ['symbol'], 
#     engine='pyarrow',
#     existing_data_behavior='delete_matching',
#     version='2.4',
#     )

# handle = dhpq.read(parquet_root)


# df2 = dhpd.to_table(pd.read_parquet(parquet_root))
# mt2 = df2.meta_table


# tt = dhpq.read(parquet_root)

# ###############################
# # working with pyarrow

# from io import StringIO

# csv_string = """ETH-USD,COINBASE,2023-04-16 19:13:07.468965120,buy,0.54228366,2133.73
# ETH-USD,COINBASE,2023-04-16 19:13:07.468965120,buy,1.17165165,2133.74
# ETH-USD,COINBASE,2023-04-16 19:13:07.468965120,buy,0.50940505,2133.74
# BTC-USD,COINBASE,2023-04-16 19:13:07.518560000,buy,0.0012981,30520.22
# BTC-USD,COINBASE,2023-04-16 19:13:08.118560000,buy,0.003981,30521.22
# """

# df = pd.read_csv(StringIO(csv_string), header=None, names=['symbol','exchange','ts','side','size','price'])
# df['ts'] = pd.to_datetime(df['ts'])
# test_table = dhpd.to_table(df)


