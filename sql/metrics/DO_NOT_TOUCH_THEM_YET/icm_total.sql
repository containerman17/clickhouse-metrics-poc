-- ICM (Interchain Messaging) total metrics
-- Parameters: chain_id, first_period, last_period, granularity

-- ICM total table
CREATE TABLE IF NOT EXISTS icm_total_{granularity} (
    chain_id UInt32,
    period DateTime64(3, 'UTC'),  -- Period start time
    value UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (chain_id, period);

-- Insert ICM total counts
-- Counts all ICM messages (both sent and received) by looking for either topic0 in logs
INSERT INTO icm_total_{granularity} (chain_id, period, value)
SELECT
    {chain_id:UInt32} as chain_id,
    toStartOf{granularity}(block_time) as period,
    count(*) as value
FROM raw_logs
WHERE chain_id = {chain_id:UInt32}
  AND block_time >= {first_period:DateTime}
  AND block_time < {last_period:DateTime}
  AND topic0 IN (
    unhex('2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8'), -- SEND
    unhex('292ee90bbaf70b5d4936025e09d56ba08f3e421156b6a568cf3c2840d9343e34')  -- RECEIVE
  )
GROUP BY period
ORDER BY period;

