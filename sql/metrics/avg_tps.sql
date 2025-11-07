-- Average Transactions Per Second metric
-- Parameters: chain_id, first_period, last_period, granularity

CREATE TABLE IF NOT EXISTS avg_tps_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert average TPS values
-- Calculates average TPS - total transactions divided by seconds in the period
-- Period duration is calculated dynamically to handle variable month lengths
INSERT INTO avg_tps_{granularity} (chain_id, period, value)
WITH period_data AS (
    SELECT
        toStartOf{granularity}(block_time) as period,
        count(*) as tx_count
    FROM raw_txs
    WHERE chain_id = {chain_id:UInt32}
      AND block_time >= {first_period:DateTime}
      AND block_time < {last_period:DateTime}
    GROUP BY period
)
SELECT
    {chain_id:UInt32} as chain_id,
    period,
    CAST(tx_count / (toUnixTimestamp(period + INTERVAL 1 {granularity}) - toUnixTimestamp(period)) AS UInt64) as value
FROM period_data
ORDER BY period;

