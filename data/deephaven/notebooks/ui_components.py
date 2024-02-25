import deephaven.plot.express as dx
import deephaven.ui as ui
from deephaven.ui import use_state
from deephaven.plot.figure import Figure


stocks = dx.data.stocks()

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#form-with-submit
@ui.component
def form_submit_example():
    def handle_submit(data):
        print(f"Hello {data['name']}, you are {data['age']} years old")

    return ui.form(
        ui.text_field(default_value="Douglas", name="name"),
        ui.number_field(default_value=42, name="age"),
        ui.button("Submit", type="submit"),
        on_submit=handle_submit,
    )


fs = form_submit_example()

###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#form-two-variables
@ui.component
def form_example():
    name, set_name = use_state("Homer")
    age, set_age = use_state(36)

    return ui.flex(
        ui.text_field(value=name, on_change=set_name),
        ui.slider(value=age, on_change=set_age),
        ui.text(f"Hello {name}, you are {age} years old"),
        direction="column",
    )


fe = form_example()

###############################################################################################################################
@ui.component
def button_event_printer(*children, id="My Button"):
    return ui.action_button(
        *children,
        on_key_down=print,
        on_key_up=print,
        on_press=print,
        on_press_start=print,
        on_press_end=print,
        on_press_change=lambda is_pressed: print(f"{id} is_pressed: {is_pressed}"),
        on_press_up=print,
        on_focus=print,
        on_blur=print,
        on_focus_change=lambda is_focused: print(f"{id} is_focused: {is_focused}"),
        id=id,
    )


@ui.component
def button_events():
    return [
        button_event_printer("1", id="My Button 1"),
        button_event_printer("2", id="My Button 2"),
    ]


be = button_events()


###############################################################################################################################
# https://github.com/deephaven/deephaven-plugins/blob/main/plugins/ui/examples/README.md#plot-with-filters
@ui.component
def stock_widget_plot(source, default_sym="", default_exchange=""):
    sym, set_sym = use_state(default_sym)
    exchange, set_exchange = use_state(default_exchange)

    ti1 = ui.text_field(
        label="Sym", label_position="side", value=sym, on_change=set_sym
    )
    ti2 = ui.text_field(
        label="Exchange", label_position="side", value=exchange, on_change=set_exchange
    )
    t1 = source.where([f"sym=`{sym.upper()}`", f"exchange=`{exchange}`"])
    p = (
        Figure()
        .plot_xy(series_name=f"{sym}-{exchange}", t=t1, x="timestamp", y="price")
        .show()
    )

    return ui.flex(ui.flex(ti1, ti2), t1, p, direction="column", flex_grow=1)


swp = stock_widget_plot(stocks, "CAT", "TPET")

