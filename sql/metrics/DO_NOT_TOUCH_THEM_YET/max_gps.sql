-- Maximum Gas Per Second metric
-- Parameters: chain_id, first_period, last_period, granularity

CREATE TABLE IF NOT EXISTS max_gps_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert max GPS values
-- Calculates max GPS per period - sum gas used per second and take max
INSERT INTO max_gps_{granularity} (chain_id, period, value)
WITH gas_per_second AS (
    SELECT 
        toStartOf{granularity}(block_time) as period,
        toStartOfSecond(block_time) as second,
        sum(gas_used) as gas_used
    FROM raw_blocks
    WHERE chain_id = {chain_id:UInt32}
      AND block_time >= {first_period:DateTime}
      AND block_time < {last_period:DateTime}
    GROUP BY period, second
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    max(gas_used) as value
FROM gas_per_second
GROUP BY period
ORDER BY period;

