# IndexRunner - Complete Rewrite

## Overview
The metrics system has been completely rewritten as `IndexRunner` to handle three distinct types of blockchain indexing with **zero race conditions**.

## Key Architecture Decisions

### 1. One IndexRunner Per Chain
**Critical**: Each chain gets its own `IndexRunner` instance.
- No shared state between chains
- No locks needed
- Chains process completely independently
- ChainSyncer creates it: `indexRunner, err := indexer.NewIndexRunner(chainID, conn, ".")`

### 2. OnBlock Signature Changed
```go
// Before (had race conditions)
metricsRunner.OnBlock(blockTime, chainId)

// After (per-chain, no races)
indexRunner.OnBlock(blockNumber, blockTime)
```
The `chainId` is set at construction time, not passed to `OnBlock`.

## Three Index Types

### Type 1: Metrics (Time-based with Granularities)
**Location**: `sql/metrics/*.sql`

**Characteristics**:
- Processes complete time periods (hour, day, week, month)
- Watermark: DateTime (period start time)
- Trigger: When next period's block arrives
- Can process multiple periods if behind

**SQL Variables**:
- `{chain_id:UInt32}` - Chain ID (uint32)
- `{first_period:DateTime}` - Start of period range
- `{last_period:DateTime}` - End of period range (exclusive)
- `{granularity}` - Literal: hour/day/week/month
- `toStartOf{granularity}` - Becomes: toStartOfHour, toStartOfDay, etc.
- `_{granularity}` - Becomes: _hour, _day, etc.

**Watermark Table**: `metric_watermarks`

### Type 2: Batched Incrementals (Block-based)
**Location**: `sql/incremental/batched/*.sql`

**Characteristics**:
- Processes blocks from watermark to current
- Watermark: Block number (uint64)
- Trigger: Max once every 5 minutes (wall clock)
- Idempotent: Runs first, then updates watermark

**SQL Variables**:
- `{chain_id:UInt32}` - Chain ID
- `{first_period:DateTime}` - Time of first block in range
- `{last_period:DateTime}` - Time of last block in range

**Watermark Table**: `batched_watermarks`

**Example**: `address_on_chain.sql` - Tracks which addresses appear on which chains

### Type 3: Immediate Incrementals
**Location**: `sql/incremental/immediate/*.sql`

**Characteristics**:
- No watermark (queries latest data)
- Trigger: Every batch, min 0.9s spacing per index
- Runs on most recent data

**SQL Variables**:
- `{chain_id:UInt32}` - Chain ID

**No watermark table** - These always query the latest data

## Implementation Details

### Throttling
- **Batched**: Wall-clock based, 5 minutes between runs
- **Immediate**: Per-index throttle, 0.9 seconds minimum

```go
// Batched throttle
if time.Since(r.lastBatchedRun) < 5*time.Minute {
    return nil
}

// Immediate throttle (per index)
lastRun, exists := r.immediateLastRun[indexName]
if exists && time.Since(lastRun) < 900*time.Millisecond {
    continue
}
```

### Error Handling
- Errors are logged but don't stop other indexes
- Each index type processes independently
- Watermarks ensure safe reprocessing

### Period Sealing Logic
Metrics use "complete period" logic:
```go
// Period is complete when we have data from the NEXT period
func isPeriodComplete(periodStart, latestBlockTime, granularity) bool {
    periodEnd := nextPeriod(periodStart, granularity)
    return latestBlockTime >= periodEnd
}
```

## Integration

In `ChainSyncer`:

```go
// Construction
indexRunner, err := indexer.NewIndexRunner(cfg.ChainID, cfg.CHConn, ".")
cs.indexRunner = indexRunner

// After writing blocks
blockTime := time.Unix(int64(timestamp), 0).UTC()
err := cs.indexRunner.OnBlock(uint64(blockNumber), blockTime)
```

## Directory Structure

```
pkg/indexer/
  - index_runner.go      # Main logic
  - period_utils.go      # Time utilities
  - watermarks.sql       # Table schemas
  - README.md            # Documentation

sql/
  - metrics/             # Time-based metrics
    - active_addresses.sql
    - active_senders.sql
    - ...
  - incremental/
    - batched/           # Block-based, throttled
      - address_on_chain.sql
    - immediate/         # Real-time, no watermark
      (add files here)
```

## Watermark Tables

```sql
-- Metrics (time-based)
CREATE TABLE metric_watermarks (
    chain_id UInt32,
    metric_name String,        -- e.g., "active_addresses_day"
    last_period DateTime64(3),
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, metric_name);

-- Batched incrementals (block-based)
CREATE TABLE batched_watermarks (
    chain_id UInt32,
    index_name String,         -- e.g., "address_on_chain"
    last_block_number UInt64,
    updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, index_name);
```

## Benefits

1. **Zero race conditions**: Per-chain design
2. **Clear separation**: Three distinct processing modes
3. **Simple throttling**: Wall-clock based
4. **Type-safe watermarks**: Block numbers vs timestamps
5. **Idempotent**: Safe to reprocess
6. **Concurrent chains**: Independent processing
7. **Flexible**: Easy to add new indexes

## Migration Notes

- Old package `pkg/metrics` completely removed
- All imports updated to `pkg/indexer`
- ChainSyncer now passes `blockNumber` to `OnBlock`
- Build and tests pass successfully
