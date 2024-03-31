
-- uncomment these to start from scratch
--DROP TABLE IF EXISTS cryptofeed.orderbooks;
--DROP TABLE IF EXISTS cryptofeed.orderbooks_queue;
--DROP VIEW IF EXISTS cryptofeed.orderbooks_1sec_mv;
--DROP VIEW IF EXISTS cryptofeed.orderbooks_all_mv;

--DROP TABLE IF EXISTS cryptofeed.orderbooks_out_queue;
--DROP VIEW IF EXISTS cryptofeed.orderbooks_out_queue_mv;

-- create database schema
CREATE DATABASE IF NOT EXISTS cryptofeed;

-----------------------------------------------------------------------------------------------------------
-- create Kafka table engine, flush every 1000ms
CREATE OR REPLACE TABLE cryptofeed.orderbooks_queue
(
  exchange      LowCardinality(String)
  , symbol      LowCardinality(String)
  , ts          DateTime64(9)
  , bid         Map(String, Float64)
  , ask         Map(String, Float64)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:29092',
    kafka_topic_list = 'orderbooks',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 1;

-----------------------------------------------------------------------------------------------------------
-- create AggregatingMergeTree, keeps the LAST orderbook snapshot per second (once merging finalizes)
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_1sec_mv
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts_bin        DateTime64(9)
    , bid           SimpleAggregateFunction(anyLast, Map(String, Float64))
    , ask           SimpleAggregateFunction(anyLast, Map(String, Float64))
)
ENGINE = AggregatingMergeTree
ORDER BY (exchange, symbol, ts_bin)
AS SELECT
  exchange
  , symbol
  , toStartOfSecond(ts) AS ts_bin
  , argMax(bid, ts)     AS bid
  , argMax(ask, ts)     AS ask
FROM cryptofeed.orderbooks_queue
GROUP BY (exchange, symbol, ts_bin);

-----------------------------------------------------------------------------------------------------------
-- create MergeTree, stores ALL orderbook snapshots (only keeps 1 HOUR of history)
CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks
(
    exchange        LowCardinality(String)
    , symbol        LowCardinality(String)
    , ts            DateTime64(9)
    , bid           Map(String, Float64)
    , ask           Map(String, Float64)
) Engine = MergeTree
PARTITION BY toYYYYMM(ts)
TTL toDateTime(ts + INTERVAL 1 HOUR)
ORDER BY (symbol, exchange, ts);

-- create materialized view, pushing ALL orderbooks snapshots from Kafka to cryptofeed.orderbooks
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_all_mv TO cryptofeed.orderbooks AS
SELECT * FROM cryptofeed.orderbooks_queue;


---------------------------------------------------------------------------------------------------------------------------------------
-- everything above this line pushes ALL orderbook snapshots into 'cryptofeed.orderbooks' and 'cryptofeed.orderbooks_1sec_mv'
-- below this line creates an outbound Kafka topic that pushes from Clickhouse back out to Kafka (experimental)
-- to do this, we follow the steps in https://clickhouse.com/docs/en/integrations/kafka/kafka-table-engine#2-using-materialized-views

-- outbound Kafka topic 'orderbooks_1sec'
--CREATE TABLE IF NOT EXISTS cryptofeed.orderbooks_out_queue
--(
--    exchange        LowCardinality(String)
--    , symbol        LowCardinality(String)
--    , ts_latest     UInt64
--    , ts_1sec       UInt64
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
-- aggregates orderbook snapshots to 1sec resolution and pushes them onto the outgoing Kafka queue
--CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.orderbooks_out_queue_mv TO cryptofeed.orderbooks_out_queue AS
--SELECT
--  exchange
--  , symbol
--  , toUnixTimestamp64Nano(max(ts))                  AS ts_latest
--  , toUnixTimestamp64Nano(max(toStartOfSecond(ts))) AS ts_1sec
--  , argMax(bid, ts)                                 AS bid
--  , argMax(ask, ts)                                 AS ask
--FROM cryptofeed.orderbooks
--WHERE
--  ts >= ts - INTERVAL 15 SECOND
--GROUP BY exchange, symbol;
