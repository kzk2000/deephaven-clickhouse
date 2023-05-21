
-- uncomment these to start from scratch
--DROP TABLE cryptofeed.trades;
--DROP TABLE cryptofeed.trades_queue;
--DROP VIEW cryptofeed.trades_queue_mv;


-- create database schema
CREATE DATABASE IF NOT EXISTS cryptofeed;

-- create table for trades
CREATE TABLE IF NOT EXISTS cryptofeed.trades
(
    symbol LowCardinality(String),
    ts DateTime64(9),
    receipt_ts DateTime64(9),
    exchange LowCardinality(String),
    side LowCardinality(String),
    size Float64,
    price Float64,
    trade_id Int64,
    KafkaOffset Int64
) ENGINE = MergeTree
ORDER BY (symbol, ts)
PRIMARY KEY (symbol, ts)
PARTITION BY (symbol, toStartOfHour(ts));


-- create trades Kafka table engine, flush every 1000ms
CREATE TABLE IF NOT EXISTS cryptofeed.trades_queue
(
    symbol LowCardinality(String),
    ts DateTime64(9),
    receipt_ts DateTime64(9),
    exchange LowCardinality(String),
    side LowCardinality(String),
    size Float64,
    price Float64,
    trade_id Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'trades',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_flush_interval_ms = 1000,
    kafka_thread_per_consumer = 0,
    kafka_num_consumers = 1;

-- materialized view that persists the Kafka table engine to cryptofeed.trades
-- _offset is a virtual column, see https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka#virtual-columns
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.trades_queue_mv TO cryptofeed.trades AS
SELECT *, _offset AS KafkaOffset FROM cryptofeed.trades_queue;


-- you can further send out the materialized via back out to another Kafka topic,
-- see https://clickhouse.com/docs/en/integrations/kafka#2-utilizing-materialized-views