# Deephaven + Clickhouse
TLDR;
Deephaven Community doesn't provide a built-in persistent storage layer (as of Apr 2023), so let's use 
[Clickhouse](https://clickhouse.com/) to create one.<br><br>
This repo shows how to 
* leverage [Cryptofeed](https://github.com/bmoscon/cryptofeed) to subscribe to 24/7 real-time Crypto market data
* push the data onto Kafka to create data live streams
* persist the streams via the Clickhouse Kafka Table Engine
* access real-time streams and historical data from the DH UI, plus some magic to stitch them together

## General Setup 
Everything should "just work", simply run ```docker-compose up -d``` (ideally let it run 24/7 on a server)
* Deephaven UI is running at http://localhost:10000/ide/
* ClickHouse Play is running at http://localhost:8123/play
  * CLICKHOUSE_USER: default, CLICKHOUSE_PASSWORD: password
* Data will be stored locally under the `/data/[deephaven|clickhouse]` folders which are mounted into the docker images

## References
* ClickHouse + Kafka: https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka
  * [Kafka to ClickHouse](https://clickhouse.com/docs/en/integrations/kafka#kafka-to-clickhouse)
  * [ClickHouse to Kafka](https://clickhouse.com/docs/en/integrations/kafka#clickhouse-to-kafka) [doable but we don't use it yet]

* ClickHouse + Redpanda: https://redpanda.com/blog/real-time-olap-database-clickhouse-redpanda
* [ClickHouse Server in 1 minute with Docker](https://dev.to/titronium/clickhouse-server-in-1-minute-with-docker-4gf2)
* [Clickhouse Kafka Engine Virtual Columns](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka#virtual-columns)
* Older version using QuestDB: https://github.com/kzk2000/deephaven-questdb
