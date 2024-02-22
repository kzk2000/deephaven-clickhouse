"""
Snippet from https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#basic-dashboard
"""

from deephaven import ui
from deephaven.plot import express as dx
from deephaven.plot.figure import Figure

_stocks = dx.data.stocks()
_cat_stocks = _stocks.where("sym=`CAT`")
_dog_stocks = _stocks.where("sym=`DOG`")
_stocks_plot = (
    Figure()
    .plot_xy("Cat", _cat_stocks, x="timestamp", y="price")
    .plot_xy("Dog", _dog_stocks, x="timestamp", y="price")
    .show()
)

my_dash = ui.dashboard(
    ui.column(
        ui.row(
            ui.stack(ui.panel(_cat_stocks, title="Cat")),
            ui.stack(ui.panel(_dog_stocks, title="Dog")),
        ),
        ui.stack(ui.panel(_stocks_plot, title="Stocks")),
    )
)