-- Same rows and feature types as the reference table. Dictionary encoding,
-- codecs and ordering are storage optimizations, not discarded source data.
CREATE TABLE IF NOT EXISTS hw2.transactions
(
    run_id UUID,
    source_row UInt64 CODEC(Delta, ZSTD(1)),
    transaction_time DateTime('UTC') CODEC(Delta, ZSTD(1)),
    merch LowCardinality(String) CODEC(ZSTD(1)),
    cat_id LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal(18, 2) CODEC(ZSTD(1)),
    name_1 String CODEC(ZSTD(1)),
    name_2 String CODEC(ZSTD(1)),
    gender LowCardinality(String) CODEC(ZSTD(1)),
    street String CODEC(ZSTD(1)),
    one_city LowCardinality(String) CODEC(ZSTD(1)),
    us_state LowCardinality(String) CODEC(ZSTD(1)),
    post_code String CODEC(ZSTD(1)),
    lat Float64 CODEC(Gorilla, ZSTD(1)),
    lon Float64 CODEC(Gorilla, ZSTD(1)),
    population_city UInt32 CODEC(T64, ZSTD(1)),
    jobs LowCardinality(String) CODEC(ZSTD(1)),
    merchant_lat Float64 CODEC(Gorilla, ZSTD(1)),
    merchant_lon Float64 CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY run_id
ORDER BY (us_state, transaction_time, source_row);

-- The maximum of (amount, -source_row) picks the first CSV row on ties.
-- States can be merged correctly even before background merges finish.
CREATE TABLE IF NOT EXISTS hw2.state_max
(
    run_id UUID,
    us_state LowCardinality(String),
    winner AggregateFunction(
        argMax,
        Tuple(String, Decimal(18, 2), UInt64),
        Tuple(Decimal(18, 2), Int64)
    )
)
ENGINE = AggregatingMergeTree
PARTITION BY run_id
ORDER BY us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.state_max_mv
TO hw2.state_max
AS SELECT
    run_id,
    us_state,
    argMaxState(
        tuple(toString(cat_id), amount, source_row),
        tuple(amount, -toInt64(source_row))
    ) AS winner
FROM hw2.transactions
GROUP BY run_id, us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_optimize_mv
TO hw2.transactions
AS SELECT * FROM hw2.transactions_baseline;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_ingest_mv
TO hw2.transactions_baseline
AS SELECT * FROM hw2.transactions_queue;
