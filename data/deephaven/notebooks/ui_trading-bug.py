
from deephaven import ui, agg
from deephaven.ui import use_state
from deephaven.plot.figure import Figure
import deephaven.plot.express as dx
import json

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

# bug: my_dash does not dynamically change Panel "tab" names
my_dash = ui.dashboard(
    ui.column(
        ui.row(sti),
    )
)

#################################################
# puzzle: what's the time
from deephaven import ui, agg
import json

stocks = dx.data.stocks().rename_columns(['symbol=sym', 'ts=timestamp'])

@ui.component
def get_pivot(source, default_sym=""):

    agg_list = [
        agg.first('symbol'),
        agg.last('last_ts = ts'),
    ]

    t1 = source.agg_by(agg_list, by=['symbol'])

    def handle_row_double_press(row, data):
        print(json.dumps(data,indent=2))


    return [
        ui.panel(
            ui.table(t1).on_row_double_press(handle_row_double_press),
            title="Agg by Table",
        ),
    ]

tmp = get_pivot(stocks)



import deephaven.ui as ui
import deephaven.ui.html as html

@ui.component
def my_component():
  return html.div(
    html.h1('Hello'),
    html.span('Some red text', color='#ff0000')
  )

kk = my_component()  

