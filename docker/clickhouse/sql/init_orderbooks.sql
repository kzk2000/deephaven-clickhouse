
-- uncomment these to start from scratch
--DROP TABLE IF EXISTS cryptofeed.orderbooks;
--DROP TABLE IF EXISTS cryptofeed.orderbooks_queue;
--DROP VIEW IF EXISTS cryptofeed.orderbooks_queue_mv;
--DROP TABLE IF EXISTS cryptofeed.orderbooks_out_queue;
--DROP VIEW IF EXISTS cryptofeed.orderbooks_out_queue_mv;

-- create database schema
CREATE DATABASE IF NOT EXISTS cryptofeed;

-- create table for ALL orderbook snapshots
CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts            DateTime64(9)
    , receipt_ts    DateTime64(9)
    , bid           Map(String, Float64)
    , ask           Map(String, Float64)
    , KafkaOffset   Int64
) Engine = MergeTree
ORDER BY (symbol, ts)
PRIMARY KEY (symbol, ts)
PARTITION BY (symbol, toStartOfHour(ts));

-- create Kafka table engine, flush every 1000ms
CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks_queue
(
  raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'orderbooks',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONAsString',
    kafka_flush_interval_ms = 1000,
    kafka_thread_per_consumer = 0,
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 1;

-- materialized view that persists the Kafka table engine to cryptofeed.orderbooks
-- _offset is a virtual column, see https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka#virtual-columns
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_queue_mv TO cryptofeed.orderbooks AS
SELECT
  JSONExtractString(raw, 'exchange')                                                              AS exchange
  , JSONExtractString(raw, 'symbol')                                                              AS symbol
  , toDateTime64(JSONExtractString(raw, 'ts'), 9)                                                 AS ts
  , toDateTime64(JSONExtractString(raw, 'receipt_ts'), 9)                                         AS receipt_ts
  , cast(JSONExtractKeysAndValues(raw, 'bid', 'Float64'), 'Map(String, Float64)')                 AS bid
  , cast(JSONExtractKeysAndValues(raw, 'ask', 'Float64'), 'Map(String, Float64)')                 AS ask
  , _offset                                                                                       AS KafkaOffset
FROM cryptofeed.orderbooks_queue;


-- everything above this line pushes ALL orderbook snapshots into cryptofeed.orderbooks
-- To downsample them to only 1 snapshot per second and push that back out to Kafka
-- to do this, we follow the steps in https://clickhouse.com/docs/en/integrations/kafka#2-utilizing-materialized-views

-- outbound Kafka topic 'orderbooks_1sec', this is the one we subscribe to on DH side
CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks_out_queue
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts_latest     UInt64
    , ts_1sec       UInt64
    , bid           Map(String, Float64)
    , ask           Map(String, Float64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'orderbooks_1sec',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_flush_interval_ms = 1000,
    kafka_thread_per_consumer = 0,
    kafka_num_consumers = 1;

-- aggregates orderbook snapshots to 1sec resolution and pushes them onto the outgoing Kafka queue
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_out_queue_mv TO cryptofeed.orderbooks_out_queue AS
SELECT
  exchange
  , symbol
  , toUnixTimestamp64Nano(max(ts))                  AS ts_latest
  , toUnixTimestamp64Nano(max(toStartOfSecond(ts))) AS ts_1sec
  , argMax(bid, ts)                                 AS bid
  , argMax(ask, ts)                                 AS ask
FROM cryptofeed.orderbooks
WHERE
  ts >= ts - INTERVAL 15 SECOND
GROUP BY exchange, symbol;


-- older version: keep for reference for now
--
--CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks_out_queue
--(
--    exchange        LowCardinality(String)
--    , symbol        LowCardinality(String)
--    , ts_1sec       UInt32
--    , bid           Map(String, Float64)
--    , ask           Map(String, Float64)
--)
--ENGINE = Kafka
--SETTINGS
--    kafka_broker_list = 'redpanda:29092',
--    kafka_topic_list = 'orderbooks_1sec',
--    kafka_group_name = 'clickhouse',
--    kafka_format = 'JSONEachRow',
--    kafka_flush_interval_ms = 1000,
--    kafka_thread_per_consumer = 0,
--    kafka_num_consumers = 1;
--
---- aggregates orderbook snapshots to 1sec resolution and pushes them onto the outgoing Kafka queue
--CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_out_queue_mv TO cryptofeed.orderbooks_out_queue AS
--SELECT
--    exchange
--    , symbol
--    , toUnixTimestamp(toStartOfSecond(ts)) AS ts
--    , toUnixTimestamp(toStartOfSecond(ts)) AS ts_1sec
--    , argMax(bid, ts)                      AS bid
--    , argMax(ask, ts)                      AS ask
--FROM cryptofeed.orderbooks
--GROUP BY ts_1sec, symbol, exchange;

