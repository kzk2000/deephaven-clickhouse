from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage
from typing import List

# Should the codebase spawn webhooks and pass market data to deephaven during market hours?
realtime_flag = True

# Equities we should maintain data about
equities = ["COIN"]

def print_handler(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

def kafka_emit_handler(object, topic):
    pass 

def subscribe_to_trades(equities = equities, handler = print_handler):
    client = WebSocketClient()
    args = [f"T.{i}" for i in equities]
    client.subscribe(*args)
    return client.run(handler)




