"""
Very slick way of to only publish when data is fully materialized - by Ryan Caudy (Deephaven CTO)
"""

from deephaven.table_factory import time_table
from deephaven import agg
from deephaven.filters import or_

current_time = time_table("PT00:00:10", blink_table=True).tail(1)

data_source = time_table("PT00:00:01").update_view([
    "value=1",
    "ts_bin=lowerBin(Timestamp, 5 * SECOND)",
])
aggregated = data_source.agg_by([agg.last("Timestamp"), agg.sum_("value")], "ts_bin")

last_bin = data_source.tail(1)

decorated = aggregated.natural_join(
    table=last_bin, on=[], joins="last_bin=ts_bin"
).natural_join(table=current_time, on=[], joins="current_time=Timestamp")


published = decorated.where(
    or_(["ts_bin < last_bin", "last_bin < current_time - 5 * SECOND"])
)  # 2nd condition such that it keeps ticking even if aggregated goes stale
