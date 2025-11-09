-- Fees paid metrics
-- Parameters: chain_id, first_period, last_period, granularity

-- Fees paid table
CREATE TABLE IF NOT EXISTS fees_paid_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt256,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert fees paid values
-- Calculates sum of transaction fees (gas_used * gas_price) per period
INSERT INTO fees_paid_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    sum(toUInt256(gas_used) * toUInt256(gas_price)) as value
FROM raw_txs
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
GROUP BY period
ORDER BY period;

