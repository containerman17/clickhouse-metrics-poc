# Indexer Rewrite Summary

## Major Changes

### 1. Renamed `pkg/metrics` → `pkg/indexer`
The metrics package has been completely rewritten and renamed to `indexer` to reflect its broader scope.

### 2. Per-Chain Architecture
**Critical change**: `IndexRunner` is now instantiated **once per chain**.

**Before**:
```go
metricsRunner.OnBlock(blockTime, chainId)  // Race conditions possible
```

**After**:
```go
indexRunner := NewIndexRunner(chainId, conn, sqlBaseDir)  // One per chain
indexRunner.OnBlock(blockNumber, blockTime)               // No chainId param
```

This eliminates ALL race conditions - each chain processes independently.

### 3. Three Index Types

#### Metrics (Time-based)
- **Path**: `sql/metrics/*.sql`
- **Granularities**: hour, day, week, month
- **Trigger**: Complete periods (when next period's block arrives)
- **Watermark**: DateTime (period start time)

#### Batched Incrementals
- **Path**: `sql/incremental/batched/*.sql`
- **Trigger**: Max once every 5 minutes (wall clock)
- **Watermark**: Block number
- **Example**: `address_on_chain.sql`

#### Immediate Incrementals
- **Path**: `sql/incremental/immediate/*.sql`
- **Trigger**: Every batch, min 0.9s spacing per index
- **Watermark**: None (runs on latest data)

### 4. Simplified Processing Logic

**Metrics**: 
- Checks for complete periods using block time
- Processes multiple periods in one run if behind
- Updates period-based watermark

**Batched**:
- Checks if 5 minutes have passed since last run
- Processes from `last_block_number + 1` to `current_block_number`
- Updates block-number watermark

**Immediate**:
- Checks if 0.9 seconds have passed for each index
- Runs SQL on latest data (no watermark)

### 5. New Watermark Tables

```sql
-- For metrics (time-based)
CREATE TABLE metric_watermarks (
    chain_id UInt32,
    metric_name String,           -- "active_addresses_day"
    last_period DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, metric_name);

-- For batched incrementals (block-based)
CREATE TABLE batched_watermarks (
    chain_id UInt32,
    index_name String,            -- "address_on_chain"
    last_block_number UInt64,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, index_name);
```

## Integration

The `ChainSyncer` now creates one `IndexRunner` per chain:

```go
// In NewChainSyncer
indexRunner, err := indexer.NewIndexRunner(cfg.ChainID, cfg.CHConn, ".")
cs.indexRunner = indexRunner

// In writeBlocks (after successful write)
blockTime := time.Unix(timestamp, 0).UTC()
cs.indexRunner.OnBlock(blockNumber, blockTime)
```

## Benefits

1. **No race conditions**: Each chain has isolated state
2. **Concurrent chains**: Chains process independently
3. **Simple throttling**: Wall-clock based, no complex coordination
4. **Type safety**: Block numbers for batched, time for metrics
5. **Clear separation**: Three distinct processing modes

## File Changes

**New files**:
- `pkg/indexer/index_runner.go` - Main indexer logic
- `pkg/indexer/period_utils.go` - Time period utilities
- `pkg/indexer/watermarks.sql` - Watermark table schemas
- `pkg/indexer/README.md` - Documentation
- `sql/incremental/immediate/` - Directory for immediate indexes

**Modified files**:
- `pkg/ingest/syncer/chain_syncer.go` - Use IndexRunner instead of MetricsRunner

**Deleted files**:
- `pkg/metrics/` - Entire package removed
