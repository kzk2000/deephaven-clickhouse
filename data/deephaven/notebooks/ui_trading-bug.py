
import deephaven.ui as ui
from deephaven.ui import use_state
from deephaven.plot.figure import Figure
import deephaven.plot.express as dx

stocks = dx.data.stocks().rename_columns(['symbol=sym', 'ts=timestamp'])


@ui.component
def stock_table_input(source, default_sym=""):
    sym, set_sym = use_state(default_sym)

    t1 = source
    t2 = source.where([f"symbol==`{sym.upper()}`"])
    p = (
        Figure()
        .plot_xy(series_name=f"{sym}", t=t2, x="ts", y="price")
        .show()
    )

    def handle_row_double_press(row, data):
        set_sym(data["symbol"]["value"])

    return [
        ui.panel(
            ui.table(t1).on_row_double_press(handle_row_double_press),
            title="ALL - Trades",
        ),
        ui.panel(t2, title=f"{sym} - Trades"),
        ui.panel(p, title=f"{sym} - Plot"),
    ]


sti = stock_table_input(stocks, "CAT")


my_dash = ui.dashboard(
    ui.column(
        ui.row(sti),
    )
)