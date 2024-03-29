
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
    exchange LowCardinality(String),
    side LowCardinality(String),
    size Float64,
    price Float64,
    trade_id Int64,
    KafkaOffset Int64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts);

-- create trades Kafka table engine, flush every 1000ms
CREATE TABLE IF NOT EXISTS cryptofeed.trades_queue
(
    symbol LowCardinality(String),
    ts DateTime64(9),
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


-- create materialized view for 1-minute candles
CREATE MATERIALIZED VIEW IF NOT EXISTS cryptofeed.candles_1min_mv
(
    symbol          LowCardinality(String)
    , ts_bin        DateTime64(9)
    , openp         SimpleAggregateFunction(any, Float64)
    , highp         SimpleAggregateFunction(max, Float64)
    , lowp          SimpleAggregateFunction(min, Float64)
    , closep        SimpleAggregateFunction(anyLast, Float64)
    , vwap          AggregateFunction(avgWeighted, Float64, Float64)
    , volume        SimpleAggregateFunction(sum, Float64)
    , volume_buys   SimpleAggregateFunction(sum, Float64)
    , volume_sells  SimpleAggregateFunction(sum, Float64)
    , ts_first      SimpleAggregateFunction(min, DateTime64(9))
    , ts_last       SimpleAggregateFunction(max, DateTime64(9))
)
ENGINE = AggregatingMergeTree
ORDER BY (symbol, ts_bin)
AS SELECT
    symbol
    , toStartOfMinute(ts)               AS ts_bin
    , argMin(price, ts)                 AS openp
    , max(price)                        AS highp
    , min(price)                        AS lowp
    , argMax(price, ts)                 AS closep
    , avgWeightedState(price, size)     AS vwap
    , sum(size)                         AS volume
    , sum(if(side = 'buy', size, 0))    AS volume_buys
    , sum(if(side = 'sell', size, 0))   AS volume_sells
    , min(ts)                           AS ts_first
    , max(ts)                           AS ts_last
FROM cryptofeed.trades_queue
GROUP BY (symbol, ts_bin);

-- TODO
-- you can further send out the materialized via back out to another Kafka topic,
-- see https://clickhouse.com/docs/en/integrations/kafka#2-utilizing-materialized-views