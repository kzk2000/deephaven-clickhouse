from deephaven.column import int_col, string_col
import deephaven.plot.express as dx
from deephaven import new_table, agg

# bar chart
#fig_bar = dx.bar(table=last_trade, x="symbol", y="size")

agg_list = [
    agg.sum_("volume = size"),
]

gg = trades.agg_by(agg_list, by= ['exchange', 'side']).snapshot()

fig_sunburst = dx.sunburst(
    gg,
    names='side',
    parents='exchange',
    values='volume',
)


# sunburst example
data = new_table([
    string_col("character", ["Eve", "Cain", "Seth", "Enos", "Noam", "Abel", "Awan", "Enoch", "Azura"]),
    string_col("parent",    ["",    "Eve",  "Eve",  "Seth", "Seth", "Eve",  "Eve",  "Awan",  "Eve" ]),
    int_col("value", [10, 14, 12, 10, 2, 6, 6, 4, 4]),
])

fig_sunburst = dx.sunburst(
    data,
    names='character',
    parents='parent',
    values='value',
)
