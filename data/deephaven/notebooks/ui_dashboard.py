import deephaven.plot.express as dx
import deephaven.ui as ui

from deephaven.plot.figure import Figure
from deephaven.ui import use_state


# ###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#plot-with-filters
@ui.component
def stock_widget_plot(source, default_sym="", default_exchange=""):
    sym, set_sym = use_state(default_sym)
    exchange, set_exchange = use_state(default_exchange)

    ti1 = ui.text_field(
        label="Symbol", label_position="side", value=sym, on_change=set_sym
    )
    ti2 = ui.text_field(
        label="Exchange", label_position="side", value=exchange, on_change=set_exchange
    )
    t1 = source.where([f"symbol==`{sym.upper()}`", f"exchange==`{exchange}`"])
    p = (
        Figure()
        .plot_xy(series_name=f"{sym}-{exchange}", t=t1, x="ts", y="price")
        .show()
    )

    return ui.flex(ui.flex(ti1, ti2), t1, p, direction="column", flex_grow=1)


trades_plot = stock_widget_plot(trades, "BTC-USD", "COINBASE")


############################################################################
# @ui.component
# def table_tabs(source):
#     return ui.tabs(
#         ui.tab_list(
#             ui.item(ui.icon("vsBell"), "ALL", key="Unfiltered"),
#             ui.item(ui.icon("vsGithubAlt"), "BTC-USD", key="BTC-USD"),
#             ui.item("ETH-USD", key="ETH-USD"),
#             ui.item("SOL-USD", key="SOL-USD"),
#             ui.item("AVAX-USD", key="AVAX-USD"),

#         ),
#         ui.tab_panels(
#             ui.item(source, key="Unfiltered"),
#             ui.item(source.where("symbol=`BTC-USD`"), key="BTC-USD"),
#             ui.item(source.where("symbol=`ETH-USD`"), key="ETH-USD"),
#             ui.item(source.where("symbol=`SOL-USD`"), key="SOL-USD"),
#             ui.item(source.where("symbol=`AVAX-USD`"), key="AVAX-USD"),
#         ),
#         flex_grow=1,
#     )


# trades_tabs = table_tabs(trades)




@ui.component
def stock_table_input(source, default_sym=""):
    sym, set_sym = use_state(default_sym)

    t1 = source.drop_columns(['is_db', 'trade_id']).sort(['ts'])
    t1_last = t1.last_by(['symbol']).sort(['symbol'])

    t2 = source.where([f"symbol==`{sym.upper()}`"])
    p = (
        Figure()
        .plot_xy(series_name=f"{sym}", t=t2, x="ts", y="price")
        .show()
    )

    query_history = f"""
    SELECT * FROM cryptofeed.trades
    WHERE
        ts >= now() - INTERVAL 60 MINUTE
        AND symbol = '{sym}'
    ORDER BY ts ASC
    """
    trades_clickhouse = tools.query_clickhouse(query_history)

    p2 = dx.scatter(
        trades_clickhouse,
        x="ts", 
        y=["price"],
        color_discrete_sequence=["red", "lightgreen", "lightblue"],
        # color_discrete_map = dict(COINBASE= "green"),
        # line_shape = 'hv',
        size_sequence=5,
        title="Trades (last 10 minutes)",
        xaxis_titles = '',
        yaxis_titles = 'Price ($)',
        by=["exchange"], 
    )

    def handle_row_double_press(row, data):
        set_sym(data["symbol"]["value"])

    def handle_on_cell_press(data):
        print("column press")
        print(data)

    return [
        ui.column(
            ui.row(
                ui.stack(
                    ui.panel(
                        ui.table(t1_last, on_row_double_press=handle_row_double_press, on_column_press=handle_on_cell_press), 
                        title="Last Trade"
                    ),
                    width=30,
                ),
                ui.stack(
                    ui.panel(
                        t2, 
                        title=f"Filtered Trades",
                    ),
                ),
                ui.stack(
                    ui.panel(
                        trades_clickhouse, 
                        title=f"Clickhouse Trades",
                    ),
                ),
            ),
            ui.row(
                ui.panel(p, title="DH Table Trades"),
                ui.panel(p2, title="Clickhouse Trades"),
            )
        ),
    ]
    
    

sti = stock_table_input(trades, "BTC-USD")


my_dash = ui.dashboard(sti)
      