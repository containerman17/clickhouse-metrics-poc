-- Unique deployers metrics (regular and cumulative)
-- Parameters: chain_id, first_period, last_period, granularity

-- Regular deployers table
CREATE TABLE IF NOT EXISTS deployers_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert regular deployer counts
-- Counts unique addresses that deployed contracts
INSERT INTO deployers_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    uniq(from) as value
FROM raw_traces
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
  AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
  AND tx_success = true
  AND from != unhex('0000000000000000000000000000000000000000')
GROUP BY period
ORDER BY period;

-- Cumulative deployers table
CREATE TABLE IF NOT EXISTS cumulative_deployers_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert cumulative deployer counts
-- Tracks total unique deployers that have ever deployed
INSERT INTO cumulative_deployers_{granularity} (chain_id, period, value)
WITH 
-- Get the last cumulative value before our range
previous_cumulative AS (
    SELECT max(value) as prev_value
    FROM cumulative_deployers_{granularity} FINAL
    WHERE chain_id = {chain_id:UInt32}
      AND period < {first_period:DateTime}
),
-- Get deployers who deployed before our period range
deployers_before AS (
    SELECT DISTINCT from as deployer
    FROM raw_traces
    WHERE chain_id = {chain_id:UInt32}
      AND block_time < {first_period:DateTime}
      AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
      AND tx_success = true
      AND from != unhex('0000000000000000000000000000000000000000')
),
-- Get NEW deployers that first deploy in our period range
new_deployer_first_seen AS (
    SELECT 
        from as deployer,
        min(toStartOf{granularity}(block_time)) as first_deploy_period
    FROM raw_traces
    WHERE chain_id = {chain_id:UInt32}
      AND block_time >= {first_period:DateTime}
      AND block_time < {last_period:DateTime}
      AND call_type IN ('CREATE', 'CREATE2', 'CREATE3')
      AND tx_success = true
      AND from != unhex('0000000000000000000000000000000000000000')
      AND from NOT IN (SELECT deployer FROM deployers_before)
    GROUP BY deployer
),
period_new AS (
    -- Count new deployers per period
    SELECT 
        first_deploy_period as period,
        count(*) as new_deployers
    FROM new_deployer_first_seen
    GROUP BY period
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    -- Add previous cumulative value to our running sum
    ifNull((SELECT prev_value FROM previous_cumulative), 0) + 
    sum(new_deployers) OVER (ORDER BY period) as value
FROM period_new
ORDER BY period;

