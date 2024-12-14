import pyarrow.parquet as pq
import deephaven.arrow as dhaw
import deephaven.parquet as dhpq


test_table = trades.tail(1000).snapshot()

root_folder = "/data/parquet/testing/"
arrow_table = dhaw.to_arrow(test_table)

# write partitioned parquet files
pq.write_to_dataset(
    arrow_table,
    root_folder,
    partition_cols=["symbol"],
    existing_data_behavior="delete_matching",
    version="2.6",
)  # version="2.6" is required to save nanosecond timestamps

dh_table = dhpq.read(root_folder, is_refreshing=True)
btc_only = dh_table.where(["symbol==`BTC-USD`"])
