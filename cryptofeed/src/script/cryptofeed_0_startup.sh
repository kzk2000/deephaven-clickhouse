#!/bin/bash

# Start the first process
python /src/script/cryptofeed_1_trades.py &

# Start the second process
python /src/script/cryptofeed_2_orderbooks.py &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?