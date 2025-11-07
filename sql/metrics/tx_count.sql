-- Transaction count metrics (regular and cumulative)
-- Parameters: chain_id, first_period, last_period, granularity

-- Regular transaction count table
CREATE TABLE IF NOT EXISTS tx_count_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert regular transaction counts
-- IMPORTANT: Count ALL transactions in each period, regardless of when they arrived
-- This ensures idempotent processing - same result every time
INSERT INTO tx_count_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    count(*) as value
FROM raw_txs
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
GROUP BY period
ORDER BY period;

-- Cumulative transaction count table
CREATE TABLE IF NOT EXISTS cumulative_tx_count_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert cumulative transaction counts (uses the regular counts we just calculated)
INSERT INTO cumulative_tx_count_{granularity} (chain_id, period, value)
WITH 
-- Get the last cumulative value before our range
previous_cumulative AS (
    SELECT max(value) as prev_value
    FROM cumulative_tx_count_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period < {first_period:DateTime}
),
-- Get counts from regular table for our period range (use FINAL to get deduplicated values)
period_counts AS (
    SELECT 
        period,
        value as period_count
    FROM tx_count_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period >= {first_period:DateTime}
      AND period < {last_period:DateTime}
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    -- Add previous cumulative value to our running sum
    ifNull((SELECT prev_value FROM previous_cumulative), 0) + 
    sum(period_count) OVER (ORDER BY period) as value
FROM period_counts
ORDER BY period;