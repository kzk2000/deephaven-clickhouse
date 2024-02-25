import deephaven.plot.express as dx
import deephaven.ui as ui
from deephaven.ui import use_state


stocks = dx.data.stocks()


###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#table-with-input-filter
@ui.component
def text_filter_table(source, column):
    value, set_value = use_state("FISH")
    t = source.where(f"{column}=`{value}`")
    return ui.flex(
        ui.text_field(value=value, on_change=set_value),
        t,
        direction="column",
        flex_grow=1,
    )

pp = text_filter_table(stocks, "sym")


###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#table-with-range-filter
@ui.component
def range_table(source, column):
    range, set_range = use_state({"start": 1000, "end": 10000})
    t = source.where(f"{column} >= {range['start']} && {column} <= {range['end']}")
    return ui.flex(
        ui.range_slider(
            value=range, on_change=set_range, label=column, min_value=0, max_value=50000
        ),
        t,
        direction="column",
        flex_grow=1,
    )


srt = range_table(stocks, "size")

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#table-with-required-filters
@ui.component
def stock_widget_table(source, default_sym="", default_exchange=""):
    sym, set_sym = use_state(default_sym)
    exchange, set_exchange = use_state(default_exchange)

    ti1 = ui.text_field(
        label="Sym", label_position="side", value=sym, on_change=set_sym
    )
    ti2 = ui.text_field(
        label="Exchange", label_position="side", value=exchange, on_change=set_exchange
    )
    error_message = ui.illustrated_message(
        ui.icon("vsWarning", style={"fontSize": "48px"}),
        ui.heading("Invalid Input"),
        ui.content("Please enter 'Sym' and 'Exchange' above"),
    )
    t1 = (
        source.where([f"sym=`{sym.upper()}`", f"exchange=`{exchange.upper()}`"])
        if sym and exchange
        else error_message
    )

    return ui.flex(ui.flex(ti1, ti2), t1, direction="column", flex_grow=1)


swt = stock_widget_table(stocks, "", "")

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#stock-rollup
import deephaven.ui as ui
from deephaven.ui import use_memo, use_state
from deephaven import agg
import deephaven.plot.express as dx

stocks = dx.data.stocks()


def get_by_filter(**byargs):
    """
    Gets a by filter where the arguments are all args passed in where the value is true.
    e.g.
    get_by_filter(sym=True, exchange=False) == ["sym"]
    get_by_filter(exchange=False) == []
    get_by_filter(sym=True, exchange=True) == ["sym", "exchange"]

    """
    return [k for k in byargs if byargs[k]]


@ui.component
def stock_table(source):
    is_sym, set_is_sym = use_state(False)
    is_exchange, set_is_exchange = use_state(False)
    highlight, set_highlight = use_state("")
    aggs, set_aggs = use_state(agg.avg(cols=["size", "price", "dollars"]))

    by = get_by_filter(sym=is_sym, exchange=is_exchange)

    formatted_table = use_memo(
        lambda: source.format_row_where(f"sym=`{highlight}`", "LEMONCHIFFON"),
        [source, highlight],
    )
    rolled_table = use_memo(
        lambda: (
            formatted_table
            if len(by) == 0
            else formatted_table.rollup(aggs=aggs, by=by)
        ),
        [formatted_table, aggs, by],
    )

    return ui.flex(
        ui.flex(
            ui.toggle_button(ui.icon("vsSymbolMisc"), "By Sym", on_change=set_is_sym),
            ui.toggle_button(
                ui.icon("vsBell"), "By Exchange", on_change=set_is_exchange
            ),
            (
                ui.fragment(
                    ui.text_field(
                        label="Highlight Sym",
                        label_position="side",
                        value=highlight,
                        on_change=set_highlight,
                    ),
                    ui.contextual_help(
                        ui.heading("Highlight Sym"),
                        ui.content("Enter a sym you would like highlighted."),
                    ),
                )
                if not is_sym and not is_exchange
                else None
            ),
            align_items="center",
            gap="size-100",
            margin="size-100",
            margin_bottom="0",
        ),
        rolled_table,
        direction="column",
        flex_grow=1,
    )


st = stock_table(stocks)

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#tabs
from deephaven import ui
from deephaven.plot import express as dx


@ui.component
def table_tabs(source):
    return ui.tabs(
        ui.tab_list(
            ui.item("Unfiltered", key="Unfiltered"),
            ui.item(ui.icon("vsAdd"), "CAT", key="CAT"),
            ui.item(ui.icon("vsMute"), "DOG", key="DOG"),
        ),
        ui.tab_panels(
            ui.item(source, key="Unfiltered"),
            ui.item(source.where("sym=`CAT`"), key="CAT"),
            ui.item(source.where("sym=`DOG`"), key="DOG"),
        ),
        flex_grow=1,
    )


tt = table_tabs(stocks.tail(100))



###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#listening-to-table-updates
import deephaven.ui as ui
from deephaven.table import Table
from deephaven import time_table, empty_table, merge
from deephaven import pandas as dhpd
import pandas as pd


def to_table(update):
    return dhpd.to_table(pd.DataFrame.from_dict(update))


def add_as_op(ls, t, op):
    t = t.update(f"type=`{op}`")
    ls.append(t)


@ui.component
def monitor_changed_data(source: Table):

    changed, set_changed = ui.use_state(empty_table(0))

    show_added, set_show_added = ui.use_state(True)
    show_removed, set_show_removed = ui.use_state(True)

    def listener(update, is_replay):

        to_merge = []

        if (added_dict := update.added()) and show_added:
            added = to_table(added_dict)
            add_as_op(to_merge, added, "added")

        if (removed_dict := update.removed()) and show_removed:
            removed = to_table(removed_dict)
            add_as_op(to_merge, removed, "removed")

        if to_merge:
            set_changed(merge(to_merge))
        else:
            set_changed(empty_table(0))

    ui.use_table_listener(source, listener)

    added_check = ui.checkbox(
        "Show Added", isSelected=show_added, on_change=set_show_added
    )

    removed_check = ui.checkbox(
        "Show Removed", isSelected=show_removed, on_change=set_show_removed
    )

    return [added_check, removed_check, changed]


t = time_table("PT1S").update(formulas=["X=i"]).tail(5)

monitor = monitor_changed_data(t)

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#using-table-data-hooks

import deephaven.ui as ui
from deephaven.table import Table
from deephaven import time_table, agg
import deephaven.plot.express as dx

@ui.component
def watch_lizards(source: Table):

    sold_lizards = source.where(["side in `sell`", "sym in `LIZARD`"])
    exchange_count_table = sold_lizards.view(["exchange"]).count_by(
        "count", by=["exchange"]
    )
    last_sell_table = sold_lizards.tail(1)
    max_size_and_price_table = sold_lizards.agg_by([agg.max_(cols=["size", "price"])])
    last_ten_sizes_table = sold_lizards.view("size").tail(10)
    average_sell_table = (
        sold_lizards.view(["size", "dollars"])
        .tail(100)
        .sum_by()
        .view("average = dollars/size")
    )

    exchange_count = ui.use_table_data(exchange_count_table)
    last_sell = ui.use_row_data(last_sell_table)
    max_size_and_price = ui.use_row_list(max_size_and_price_table)
    last_ten_sizes = ui.use_column_data(last_ten_sizes_table)
    average_sell = ui.use_cell_data(average_sell_table)

    exchange_count_view = ui.view(f"Exchange counts {exchange_count}")
    last_sell_view = ui.view(f"Last Sold LIZARD: {last_sell}")
    max_size_and_price_view = ui.view(f"Max size and max price: {max_size_and_price}")
    last_ten_sizes_view = ui.view(f"Last Ten Sizes: {last_ten_sizes}")
    average_sell_view = ui.view(f"Average LIZARD price: {average_sell}")

    return ui.flex(
        exchange_count_view,
        last_sell_view,
        max_size_and_price_view,
        last_ten_sizes_view,
        average_sell_view,
        margin=10,
        gap=10,
        direction="column",
    )


watch = watch_lizards(stocks)

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#multi-threading
import logging
import threading
import time
from deephaven import read_csv, ui


@ui.component
def csv_loader():
    # The render_queue we fetch using the `use_render_queue` hook at the top of the component
    render_queue = ui.use_render_queue()
    table, set_table = ui.use_state()
    error, set_error = ui.use_state()

    def handle_submit(data):
        # We define a callable that we'll queue up on our own thread
        def load_table():
            try:
                # Read the table from the URL
                t = read_csv(data["url"])

                # Define our state updates in another callable. We'll need to call this on the render thread
                def update_state():
                    set_error(None)
                    set_table(t)

                # Queue up the state update on the render thread
                render_queue(update_state)
            except Exception as e:
                # In case we have any errors, we should show the error to the user. We still need to call this from the render thread,
                # so we must assign the exception to a variable and call the render_queue with a callable that will set the error
                error_message = e

                def update_state():
                    set_table(None)
                    set_error(error_message)

                # Queue up the state update on the render thread
                render_queue(update_state)

        # Start our own thread loading the table
        threading.Thread(target=load_table).start()

    return [
        # Our form displaying input from the user
        ui.form(
            ui.flex(
                ui.text_field(
                    default_value="https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro.csv",
                    label="Enter URL",
                    label_position="side",
                    name="url",
                    flex_grow=1,
                ),
                ui.button(f"Load Table", type="submit"),
                gap=10,
            ),
            on_submit=handle_submit,
        ),
        (
            # Display a hint if the table is not loaded yet and we don't have an error
            ui.illustrated_message(
                ui.heading("Enter URL above"),
                ui.content("Enter a URL of a CSV above and click 'Load' to load it"),
            )
            if error is None and table is None
            else None
        ),
        # The loaded table. Doesn't show anything if it is not loaded yet
        table,
        # An error message if there is an error
        (
            ui.illustrated_message(
                ui.icon("vsWarning"),
                ui.heading("Error loading table"),
                ui.content(f"{error}"),
            )
            if error != None
            else None
        ),
    ]


my_loader = csv_loader()
