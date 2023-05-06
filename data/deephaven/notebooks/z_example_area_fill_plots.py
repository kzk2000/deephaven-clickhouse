# Imports
from deephaven import empty_table
from deephaven import time_table
import numpy as np
from deephaven.plot.figure import Figure, PlotStyle, Color

# Function defs
def noisy_signal(val) -> np.double:
    return 7.5 + 4.2 * np.sin(val) - 1.1 * np.cos(7 * val) + 1.5 * np.sin(1.8 * val) + 0.6 * np.cos(4 * val) + np.random.uniform(-1.5, 1.5)

def invert_with_noise(val) -> np.double:
    return -1 * val + np.random.uniform(-1, 1)

math_formulas = ["X = 0.01 * i", "Y1 = noisy_signal(X)", "Y2 = invert_with_noise(Y1)"]
y1_pct_formulas = [f"Y1_{x} = 0.{x} * Y1" for x in range(9, 0, -1)]
y2_pct_formulas = [f"Y2_{x} = 0.{x} * Y2" for x in range(9, 0, -1)]

# source = empty_table(3142).update(math_formulas + y1_pct_formulas + y2_pct_formulas)
source = time_table("00:00:00.10").update(math_formulas + y1_pct_formulas + y2_pct_formulas).tail(1000)



plot_filled_area = Figure().axes(plot_style=PlotStyle.AREA)\
    .plot_xy(series_name="Y1", t=source, x="X", y="Y1").series(name="Y1", color=Color.of_rgb(0, 255, 50))\
    .plot_xy(series_name="Y1_9", t=source, x="X", y="Y1_9").series(name="Y1_9", color=Color.of_rgb(0, 225, 45))\
    .plot_xy(series_name="Y1_8", t=source, x="X", y="Y1_8").series(name="Y1_8", color=Color.of_rgb(30, 200, 40))\
    .plot_xy(series_name="Y1_7", t=source, x="X", y="Y1_7").series(name="Y1_7", color=Color.of_rgb(60, 170, 35))\
    .plot_xy(series_name="Y1_6", t=source, x="X", y="Y1_6").series(name="Y1_6", color=Color.of_rgb(90, 140, 30))\
    .plot_xy(series_name="Y1_5", t=source, x="X", y="Y1_5").series(name="Y1_5", color=Color.of_rgb(125, 110, 25))\
    .plot_xy(series_name="Y1_4", t=source, x="X", y="Y1_4").series(name="Y1_4", color=Color.of_rgb(155, 95, 20))\
    .plot_xy(series_name="Y1_3", t=source, x="X", y="Y1_3").series(name="Y1_3", color=Color.of_rgb(185, 65, 15))\
    .plot_xy(series_name="Y1_2", t=source, x="X", y="Y1_2").series(name="Y1_2", color=Color.of_rgb(220, 30, 10))\
    .plot_xy(series_name="Y1_1", t=source, x="X", y="Y1_1").series(name="Y1_1", color=Color.of_rgb(255, 0, 0))\
    .plot_xy(series_name="Y2", t=source, x="X", y="Y2").series(name="Y2", color=Color.of_rgb(0, 255, 50))\
    .plot_xy(series_name="Y2_9", t=source, x="X", y="Y2_9").series(name="Y2_9", color=Color.of_rgb(0, 225, 45))\
    .plot_xy(series_name="Y2_8", t=source, x="X", y="Y2_8").series(name="Y2_8", color=Color.of_rgb(30, 200, 40))\
    .plot_xy(series_name="Y2_7", t=source, x="X", y="Y2_7").series(name="Y2_7", color=Color.of_rgb(60, 170, 35))\
    .plot_xy(series_name="Y2_6", t=source, x="X", y="Y2_6").series(name="Y2_6", color=Color.of_rgb(90, 140, 30))\
    .plot_xy(series_name="Y2_5", t=source, x="X", y="Y2_5").series(name="Y2_5", color=Color.of_rgb(125, 110, 25))\
    .plot_xy(series_name="Y2_4", t=source, x="X", y="Y2_4").series(name="Y2_4", color=Color.of_rgb(155, 95, 20))\
    .plot_xy(series_name="Y2_3", t=source, x="X", y="Y2_3").series(name="Y2_3", color=Color.of_rgb(185, 65, 15))\
    .plot_xy(series_name="Y2_2", t=source, x="X", y="Y2_2").series(name="Y2_2", color=Color.of_rgb(220, 30, 10))\
    .plot_xy(series_name="Y2_1", t=source, x="X", y="Y2_1").series(name="Y2_1", color=Color.of_rgb(255, 0, 0))\
    .show()