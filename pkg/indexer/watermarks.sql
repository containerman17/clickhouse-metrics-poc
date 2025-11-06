-- Watermark tables for indexer

-- Metrics: time-based watermarks (for granular metrics)
CREATE TABLE IF NOT EXISTS metric_watermarks (
    chain_id UInt32,
    metric_name String,
    last_period DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, metric_name);

-- Batched incrementals: block-number-based watermarks
CREATE TABLE IF NOT EXISTS batched_watermarks (
    chain_id UInt32,
    index_name String,
    last_block_number UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, index_name);
