-- Maximum gas price metric
-- Parameters: chain_id, first_period, last_period, granularity

CREATE TABLE IF NOT EXISTS max_gas_price_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert max gas price values
-- Finds maximum gas price per period from transaction receipts
INSERT INTO max_gas_price_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    max(gas_price) as value
FROM raw_txs
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
GROUP BY period
ORDER BY period;

