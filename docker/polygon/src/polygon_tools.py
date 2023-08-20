"""
Functions to be imported by the while loops that write to Kafka
"""
from polygon import RESTClient

def spawn_client():
    return RESTClient(os.environ["POLYGON_API_KEY"))

def get_trades_iter_for_day(symbol: str, date: str):
    client = spawn_client()
    ## Example: 
    # Trade(conditions=[12, 14, 37, 41], 
    # correction=None, 
    # exchange=12, 
    # id='22834', 
    # participant_timestamp=1691625593902018160, 
    # price=84.01, 
    # sequence_number=4680660, 
    # sip_timestamp=1691625593902034598, 
    # size=20, 
    # tape=3, 
    # trf_id=None, 
    # trf_timestamp=None)
    return list_trades(ticker = symbol, timestamp = date)


