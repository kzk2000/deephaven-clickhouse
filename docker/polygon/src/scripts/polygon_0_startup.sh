#!/bin/bash

# Start the first process
python /src/script/polygon_1_trades.py

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?
