from deephaven import dtypes, parquet
from deephaven.column import Column
from deephaven.experimental import s3

from datetime import timedelta

transactions = parquet.read(
    "s3://aws-public-blockchain/v1.0/btc/transactions/date=2023-09-10/part-00000-c6c0687d-175e-41e0-a9a0-db5a61631b1e-c000.snappy.parquet",
    special_instructions=s3.S3Instructions(
        "us-east-2",
        anonymous_access=True,
        read_ahead_count=8,
        fragment_size=65536,
        read_timeout=timedelta(seconds=10),
    ),
    file_layout=parquet.ParquetFileLayout.SINGLE_FILE,
    table_definition=[
        Column("hash", dtypes.string),
        Column("version", dtypes.int64),
        Column("size", dtypes.int64),
        Column("block_hash", dtypes.string),
        Column("block_number", dtypes.int64),
        Column("index", dtypes.int64),
        Column("virtual_size", dtypes.int64),
        Column("lock_time", dtypes.int64),
        Column("input_count", dtypes.int64),
        Column("output_count", dtypes.int64),
        Column("is_coinbase", dtypes.bool_),
        Column("output_value", dtypes.float64),
        Column("last_modified", dtypes.Instant),
        Column("fee", dtypes.float64),
        Column("input_value", dtypes.float64),
    ],
)