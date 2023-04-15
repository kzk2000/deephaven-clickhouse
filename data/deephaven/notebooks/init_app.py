from deephaven.appmode import ApplicationState, get_app_state
from deephaven import time_table, empty_table
from typing import Callable


def start(app: ApplicationState):
    print('Starting app mode...')
    # size = 42
    # app["hello"] = empty_table(size)
    # app["world"] = time_table("00:00:01")


def initialize(func: Callable[[ApplicationState], None]):
    app = get_app_state()
    func(app)


initialize(start)
