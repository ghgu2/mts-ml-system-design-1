CREATE DATABASE IF NOT EXISTS hw2;

-- Reference schema: all CSV features, ordinary strings, default compression,
-- no sorting key. Each ingestion run occupies a separate partition.
CREATE TABLE IF NOT EXISTS hw2.transactions_baseline
(
    run_id UUID,
    source_row UInt64,
    transaction_time DateTime('UTC'),
    merch String,
    cat_id String,
    amount Decimal(18, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64
)
ENGINE = MergeTree
PARTITION BY run_id
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS hw2.transactions_queue
AS hw2.transactions_baseline
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'hw2-transactions',
    kafka_group_name = 'hw2-clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_flush_interval_ms = 1000;

-- The Kafka consumer is attached after the entire optimized chain exists
-- (see 02_optimized.sql), so its first batch reaches every destination.
