package indexers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IndexRunner processes all indexers for a single chain
// One instance per chain to avoid race conditions
type IndexRunner struct {
	chainId uint32
	conn    driver.Conn

	// SQL directories
	metricsDir      string
	batchedDir      string
	immediateDir    string

	// Current state
	mu              sync.Mutex
	latestBlockTime time.Time
	latestBlockNum  uint32

	// Throttling for batched incremental (5 min wall time)
	batchedLastRun map[string]time.Time

	// Throttling for immediate incremental (0.9s spacing)
	immediateLastRun map[string]time.Time
}

// NewIndexRunner creates a new indexer runner for a specific chain
func NewIndexRunner(conn driver.Conn, chainId uint32, baseSQLDir string) (*IndexRunner, error) {
	// Create watermark tables
	if err := createWatermarkTables(conn); err != nil {
		return nil, fmt.Errorf("failed to create watermark tables: %w", err)
	}

	return &IndexRunner{
		chainId:          chainId,
		conn:             conn,
		metricsDir:       filepath.Join(baseSQLDir, "metrics"),
		batchedDir:       filepath.Join(baseSQLDir, "incremental", "batched"),
		immediateDir:     filepath.Join(baseSQLDir, "incremental", "immediate"),
		batchedLastRun:   make(map[string]time.Time),
		immediateLastRun: make(map[string]time.Time),
	}, nil
}

// OnBlock updates the latest block info and processes all indexers
// blockNumber is used for incremental indexers, blockTime for metrics
func (r *IndexRunner) OnBlock(blockTime time.Time, blockNumber uint32) error {
	r.mu.Lock()
	r.latestBlockTime = blockTime
	r.latestBlockNum = blockNumber
	r.mu.Unlock()

	// Process all indexer types
	if err := r.processMetrics(); err != nil {
		return fmt.Errorf("metrics processing failed: %w", err)
	}

	if err := r.processBatchedIncremental(); err != nil {
		return fmt.Errorf("batched incremental processing failed: %w", err)
	}

	return nil
}

// OnBatch processes immediate incremental indexers after a batch is written
// This is called separately from OnBlock to ensure proper spacing
func (r *IndexRunner) OnBatch() error {
	return r.processImmediateIncremental()
}

// processMetrics processes all metrics with granularities
func (r *IndexRunner) processMetrics() error {
	files, err := os.ReadDir(r.metricsDir)
	if err != nil {
		return fmt.Errorf("failed to read metrics directory: %w", err)
	}

	r.mu.Lock()
	latestBlockTime := r.latestBlockTime
	r.mu.Unlock()

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		metricName := strings.TrimSuffix(file.Name(), ".sql")

		// Process for all granularities
		for _, granularity := range []string{"hour", "day", "week", "month"} {
			if err := r.processMetric(metricName, granularity, latestBlockTime); err != nil {
				fmt.Printf("[Indexer Chain %d] Error processing metric %s_%s: %v\n", r.chainId, metricName, granularity, err)
			}
		}
	}

	return nil
}

// processMetric processes a single metric for a given granularity
func (r *IndexRunner) processMetric(metricName, granularity string, latestBlockTime time.Time) error {
	watermarkKey := fmt.Sprintf("%s_%s", metricName, granularity)
	lastProcessed := r.getMetricWatermark(watermarkKey)

	// If never processed, get the earliest block time
	isFirstRun := lastProcessed.IsZero()
	if isFirstRun {
		earliestBlock := r.getEarliestBlockTime()
		if earliestBlock.IsZero() {
			return nil
		}
		firstDataPeriod := toStartOfPeriod(earliestBlock, granularity)
		lastProcessed = firstDataPeriod.Add(-getPeriodDuration(granularity))
		fmt.Printf("[Indexer Chain %d] Metric %s - Starting from earliest data: %s\n", r.chainId, watermarkKey, earliestBlock.Format(time.RFC3339))
	}

	// Calculate periods to process (only complete periods)
	periods := getPeriodsToProcess(lastProcessed, latestBlockTime, granularity)
	if len(periods) == 0 {
		if isFirstRun {
			r.setMetricWatermark(watermarkKey, lastProcessed)
		}
		return nil
	}

	// Read SQL file
	sqlPath := filepath.Join(r.metricsDir, metricName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	// Execute SQL with replacements
	statements := splitSQL(string(sqlBytes))
	firstPeriod := periods[0]
	lastPeriod := nextPeriod(periods[len(periods)-1], granularity)

	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		sql := r.replacePlaceholders(stmt, granularity, firstPeriod, lastPeriod, 0)

		if err := r.conn.Exec(context.Background(), sql); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	// Update watermark after successful execution
	return r.setMetricWatermark(watermarkKey, periods[len(periods)-1])
}

// processBatchedIncremental processes batched incremental indexers
// Runs on block numbers above watermark, throttled to max once per 5 minutes
func (r *IndexRunner) processBatchedIncremental() error {
	files, err := os.ReadDir(r.batchedDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, skip
		}
		return fmt.Errorf("failed to read batched directory: %w", err)
	}

	r.mu.Lock()
	latestBlockNum := r.latestBlockNum
	now := time.Now()
	r.mu.Unlock()

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		indexerName := strings.TrimSuffix(file.Name(), ".sql")

		// Check throttle (5 minutes wall time)
		r.mu.Lock()
		lastRun, exists := r.batchedLastRun[indexerName]
		r.mu.Unlock()

		if exists && now.Sub(lastRun) < 5*time.Minute {
			continue // Skip if less than 5 minutes since last run
		}

		// Get watermark (block number)
		lastBlock := r.getBatchedWatermark(indexerName)

		// Process if we have new blocks (from lastBlock+1 to latestBlockNum)
		if latestBlockNum > lastBlock {
			fromBlock := lastBlock + 1
			if err := r.runBatchedIncremental(indexerName, fromBlock, latestBlockNum); err != nil {
				fmt.Printf("[Indexer Chain %d] Error processing batched incremental %s: %v\n", r.chainId, indexerName, err)
				continue
			}

			// Update throttle time
			r.mu.Lock()
			r.batchedLastRun[indexerName] = now
			r.mu.Unlock()

			// Update watermark only after successful execution
			r.setBatchedWatermark(indexerName, latestBlockNum)
		}
	}

	return nil
}

// runBatchedIncremental executes a batched incremental indexer
func (r *IndexRunner) runBatchedIncremental(indexerName string, fromBlock, toBlock uint32) error {
	sqlPath := filepath.Join(r.batchedDir, indexerName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	// For batched incremental, we need to convert block numbers to time ranges
	// Get time range from blocks
	firstTime, lastTime, err := r.getBlockTimeRange(fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("failed to get block time range: %w", err)
	}

	// If no blocks found in range, skip execution (idempotent - safe to skip)
	if firstTime.IsZero() || lastTime.IsZero() {
		return nil
	}

	statements := splitSQL(string(sqlBytes))
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		sql := r.replacePlaceholders(stmt, "", firstTime, lastTime, toBlock)

		if err := r.conn.Exec(context.Background(), sql); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	return nil
}

// processImmediateIncremental processes immediate incremental indexers
// Runs on every batch, spaced by at least 0.9 seconds
func (r *IndexRunner) processImmediateIncremental() error {
	files, err := os.ReadDir(r.immediateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, skip
		}
		return fmt.Errorf("failed to read immediate directory: %w", err)
	}

	r.mu.Lock()
	latestBlockTime := r.latestBlockTime
	latestBlockNum := r.latestBlockNum
	now := time.Now()
	r.mu.Unlock()

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		indexerName := strings.TrimSuffix(file.Name(), ".sql")

		// Check throttle (0.9 seconds)
		r.mu.Lock()
		lastRun, exists := r.immediateLastRun[indexerName]
		r.mu.Unlock()

		if exists && now.Sub(lastRun) < 900*time.Millisecond {
			continue // Skip if less than 0.9s since last run
		}

		// Process with latest block info
		if !latestBlockTime.IsZero() {
			if err := r.runImmediateIncremental(indexerName, latestBlockTime, latestBlockNum); err != nil {
				fmt.Printf("[Indexer Chain %d] Error processing immediate incremental %s: %v\n", r.chainId, indexerName, err)
				continue
			}

			// Update throttle time
			r.mu.Lock()
			r.immediateLastRun[indexerName] = now
			r.mu.Unlock()
		}
	}

	return nil
}

// runImmediateIncremental executes an immediate incremental indexer
func (r *IndexRunner) runImmediateIncremental(indexerName string, blockTime time.Time, blockNum uint32) error {
	sqlPath := filepath.Join(r.immediateDir, indexerName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	// Use a small time window around the block time
	firstTime := blockTime.Add(-time.Second)
	lastTime := blockTime.Add(time.Second)

	statements := splitSQL(string(sqlBytes))
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		sql := r.replacePlaceholders(stmt, "", firstTime, lastTime, blockNum)

		if err := r.conn.Exec(context.Background(), sql); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	return nil
}

// replacePlaceholders replaces all placeholders in SQL
func (r *IndexRunner) replacePlaceholders(sql, granularity string, firstTime, lastTime time.Time, blockNum uint32) string {
	sql = strings.ReplaceAll(sql, "{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId))

	if granularity != "" {
		sql = strings.ReplaceAll(sql, "toStartOf{granularity}", fmt.Sprintf("toStartOf%s", capitalize(granularity)))
		sql = strings.ReplaceAll(sql, "_{granularity}", fmt.Sprintf("_%s", granularity))
		sql = strings.ReplaceAll(sql, "{granularity}", granularity)
	}

	if !firstTime.IsZero() {
		sql = strings.ReplaceAll(sql, "{first_period:DateTime}",
			fmt.Sprintf("toDateTime64('%s', 3)", firstTime.Format("2006-01-02 15:04:05.000")))
	}

	if !lastTime.IsZero() {
		sql = strings.ReplaceAll(sql, "{last_period:DateTime}",
			fmt.Sprintf("toDateTime64('%s', 3)", lastTime.Format("2006-01-02 15:04:05.000")))
	}

	if blockNum > 0 {
		sql = strings.ReplaceAll(sql, "{block_number:UInt32}", fmt.Sprintf("%d", blockNum))
	}

	return sql
}

// Watermark management

func createWatermarkTables(conn driver.Conn) error {
	ctx := context.Background()

	// Metric watermarks (time-based)
	metricSQL := `
	CREATE TABLE IF NOT EXISTS metric_watermarks (
		chain_id UInt32,
		metric_name String,
		last_period DateTime64(3, 'UTC'),
		updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (chain_id, metric_name)`

	if err := conn.Exec(ctx, metricSQL); err != nil {
		return fmt.Errorf("failed to create metric_watermarks: %w", err)
	}

	// Batched incremental watermarks (block-based)
	batchedSQL := `
	CREATE TABLE IF NOT EXISTS batched_watermarks (
		chain_id UInt32,
		indexer_name String,
		last_block UInt32,
		updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (chain_id, indexer_name)`

	if err := conn.Exec(ctx, batchedSQL); err != nil {
		return fmt.Errorf("failed to create batched_watermarks: %w", err)
	}

	return nil
}

func (r *IndexRunner) getMetricWatermark(metricName string) time.Time {
	ctx := context.Background()
	var lastPeriod time.Time

	query := `SELECT last_period FROM metric_watermarks FINAL WHERE chain_id = ? AND metric_name = ?`
	row := r.conn.QueryRow(ctx, query, r.chainId, metricName)
	if err := row.Scan(&lastPeriod); err != nil {
		return time.Time{}
	}

	return lastPeriod
}

func (r *IndexRunner) setMetricWatermark(metricName string, lastPeriod time.Time) error {
	ctx := context.Background()
	query := `INSERT INTO metric_watermarks (chain_id, metric_name, last_period) VALUES (?, ?, ?)`
	return r.conn.Exec(ctx, query, r.chainId, metricName, lastPeriod)
}

func (r *IndexRunner) getBatchedWatermark(indexerName string) uint32 {
	ctx := context.Background()
	var lastBlock uint32

	query := `SELECT last_block FROM batched_watermarks FINAL WHERE chain_id = ? AND indexer_name = ?`
	row := r.conn.QueryRow(ctx, query, r.chainId, indexerName)
	if err := row.Scan(&lastBlock); err != nil {
		return 0
	}

	return lastBlock
}

func (r *IndexRunner) setBatchedWatermark(indexerName string, lastBlock uint32) error {
	ctx := context.Background()
	query := `INSERT INTO batched_watermarks (chain_id, indexer_name, last_block) VALUES (?, ?, ?)`
	return r.conn.Exec(ctx, query, r.chainId, indexerName, lastBlock)
}

func (r *IndexRunner) getEarliestBlockTime() time.Time {
	ctx := context.Background()
	var earliestTime time.Time

	query := `SELECT min(block_time) FROM raw_transactions WHERE chain_id = ?`
	row := r.conn.QueryRow(ctx, query, r.chainId)
	if err := row.Scan(&earliestTime); err != nil {
		return time.Time{}
	}

	return earliestTime
}

func (r *IndexRunner) getBlockTimeRange(fromBlock, toBlock uint32) (time.Time, time.Time, error) {
	ctx := context.Background()
	var firstTime, lastTime time.Time

	query := `
	SELECT 
		min(block_time) as first_time,
		max(block_time) as last_time
	FROM raw_blocks
	WHERE chain_id = ? AND block_number >= ? AND block_number <= ?`

	row := r.conn.QueryRow(ctx, query, r.chainId, fromBlock, toBlock)
	if err := row.Scan(&firstTime, &lastTime); err != nil {
		// If no blocks found, return zero times - SQL will handle empty result
		// This can happen if blocks haven't been ingested yet
		return time.Time{}, time.Time{}, nil
	}

	// If times are zero, no blocks found
	if firstTime.IsZero() || lastTime.IsZero() {
		return time.Time{}, time.Time{}, nil
	}

	return firstTime, lastTime, nil
}

// Utility functions

func splitSQL(content string) []string {
	lines := strings.Split(content, "\n")
	var cleanLines []string

	for _, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cleanLines = append(cleanLines, line)
	}

	cleaned := strings.Join(cleanLines, "\n")
	statements := strings.Split(cleaned, ";")

	var result []string
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) != "" {
			result = append(result, stmt)
		}
	}

	return result
}

func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
