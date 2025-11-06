# Indexer Package

The indexer package provides a unified framework for processing three types of indexes/metrics for blockchain data:

## Architecture

**One IndexRunner per chain** - This design eliminates race conditions by ensuring each chain has its own independent indexer.

## Index Types

### 1. Metrics (Time-based with Granularities)
- **Location**: `sql/metrics/*.sql`
- **Granularities**: hour, day, week, month
- **Watermark**: Period-based (DateTime)
- **Processing**: When a period is complete (next period's block arrives)
- **Examples**: active_addresses, active_senders, tx_count

**SQL Parameters**:
- `{chain_id:UInt32}` - Chain ID
- `{first_period:DateTime}` - Start of period range
- `{last_period:DateTime}` - End of period range (exclusive)
- `{granularity}` - Granularity name (hour/day/week/month)
- `toStartOf{granularity}` - Becomes toStartOfHour, toStartOfDay, etc.
- `_{granularity}` - Becomes _hour, _day, etc.

### 2. Batched Incrementals (Block-number based)
- **Location**: `sql/incremental/batched/*.sql`
- **Watermark**: Block number
- **Throttle**: Maximum once every 5 minutes (wall clock)
- **Processing**: Processes all blocks from last watermark to current block
- **Idempotent**: Run first, then update watermark
- **Examples**: address_on_chain

**SQL Parameters**:
- `{chain_id:UInt32}` - Chain ID
- `{first_period:DateTime}` - Start time of first block in range
- `{last_period:DateTime}` - End time of last block in range

### 3. Immediate Incrementals
- **Location**: `sql/incremental/immediate/*.sql`
- **Watermark**: None (runs on latest data)
- **Throttle**: Minimum 0.9 seconds between runs (per index)
- **Processing**: Runs on every batch, queries latest data

**SQL Parameters**:
- `{chain_id:UInt32}` - Chain ID

## Usage

```go
// Create one IndexRunner per chain
indexRunner, err := indexer.NewIndexRunner(chainID, conn, sqlBaseDir)
if err != nil {
    return err
}

// Call OnBlock after writing each batch
err = indexRunner.OnBlock(blockNumber, blockTime)
```

## Watermark Tables

Two watermark tables track processing state:

**metric_watermarks**:
```sql
CREATE TABLE metric_watermarks (
    chain_id UInt32,
    metric_name String,           -- e.g., "active_addresses_day"
    last_period DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, metric_name);
```

**batched_watermarks**:
```sql
CREATE TABLE batched_watermarks (
    chain_id UInt32,
    index_name String,            -- e.g., "address_on_chain"
    last_block_number UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, index_name);
```

## Thread Safety

- **No race conditions**: Each chain has its own IndexRunner instance
- **No locks needed**: Single-threaded per chain
- **Concurrent chains**: Different chains process independently

## Error Handling

- Errors are logged but don't stop other indexes
- Each index type (metrics/batched/immediate) is isolated
- Watermarks ensure idempotent reprocessing
