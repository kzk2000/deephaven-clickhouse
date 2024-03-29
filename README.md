# Deephaven + Clickhouse
TLDR;
Deephaven Community doesn't provide a built-in persistent storage layer (as of Spring 2024), so let's use 
[Clickhouse](https://clickhouse.com/) to create one.<br><br>
This repo shows how to 
* leverage [Cryptofeed](https://github.com/bmoscon/cryptofeed) to subscribe to 24/7 real-time Crypto market data
* push the data onto Kafka to create live streams
* persist the streams via the Clickhouse Kafka Table Engine
* access real-time streams and historical data from the DH UI, plus some magic to stitch them together
* build a Deephaven app mode around it

## General Setup 
Everything should "just work", simply run this and wait until all 5 containers start up:<br>
```
docker compose build --no-cache
docker compose up -d  
```
* Deephaven UI is running at http://localhost:10000/ide/
* ClickHouse Play is running at http://localhost:8123/play
  * CLICKHOUSE_USER: default
  * CLICKHOUSE_PASSWORD: password
* Redpanda Console is running at http://localhost:8080/overview
* Data is stored locally under the `/data/[clickhouse|deephaven|redpanda]` folders which are mounted into the docker images


## Project structure
```
├── data                                <- Project data
│   ├── clickhouse                          <- volume mount for the 'clickhouse' container
│   ├── deephaven                           <- volume mount for the 'deephaven' container
│   │   ├── layouts                         <- contains .json file for Deephaven app layout 
│   │   └── notebooks                       <- Deephaven File Explorer, all DH python scripts are stored here
│   └── redpanda                            <- volume mount for the 'redpanda' container
│
├── docker                               <- anything needed for Docker builds 
│   ├── clickhouse                          <- build files for ClickHouse                               
│   ├── cryptofeed                          <- build files for Cryptofeed 
│   ├── redpanda                            <- build files for Redpanda (e.g. Kafka retention of 1min)
│   └── deephaven                           <- build files for Deephaven
│
├── .gitignore                          <- List of files ignored by git
├── docker-compose.yaml                 <- docker-compose file to start up everything
├── requirements.txt                    <- File for installing python dependencies
├── setup.py                            <- File for installing project as a package
└── README.md
```
## References
* ClickHouse + Kafka: https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka
  * [Kafka to ClickHouse](https://clickhouse.com/docs/en/integrations/kafka#kafka-to-clickhouse)
  * [ClickHouse to Kafka](https://clickhouse.com/docs/en/integrations/kafka#clickhouse-to-kafka) [doable but we don't use it yet]

* ClickHouse + Redpanda: https://redpanda.com/blog/real-time-olap-database-clickhouse-redpanda
* [ClickHouse Server in 1 minute with Docker](https://dev.to/titronium/clickhouse-server-in-1-minute-with-docker-4gf2)
* [Clickhouse Kafka Engine Virtual Columns](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka#virtual-columns)
* Older version using QuestDB: https://github.com/kzk2000/deephaven-questdb
