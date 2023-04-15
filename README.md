# Deephaven + Clickhouse experiements
Similar to https://github.com/kzk2000/deephaven-questdb in spirit but using [Clickhouse](https://clickhouse.com/) as backend 
which allows for straightforward Kafka sink.

## General Setup 
Everything should "just work" by running ```docker-compose up -d```
* Deephave UI is running at http://localhost:10000/ide/
* ClickHouse Play is running at http://localhost:8123/play (the password is 'password')

## References
* ClickHouse + Kafka: https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka
    * [Kafka to ClickHouse](https://clickhouse.com/docs/en/integrations/kafka#kafka-to-clickhouse)
    * [ClickHouse to Kafka](https://clickhouse.com/docs/en/integrations/kafka#clickhouse-to-kafka)

* ClickHouse + Redpanda: https://redpanda.com/blog/real-time-olap-database-clickhouse-redpanda
* [ClickHouse Server in 1 minute with Docker](https://dev.to/titronium/clickhouse-server-in-1-minute-with-docker-4gf2)
* [Clickhouse Kafka Engine Virtual Columns](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka#virtual-columns)

