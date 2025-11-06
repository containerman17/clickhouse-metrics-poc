package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IndexRunner processes all types of indexes for a single chain
// One instance per chain to avoid race conditions
type IndexRunner struct {
	chainId uint32
	conn    driver.Conn

	// Directories for different index types
	metricsDir   string // sql/metrics/*.sql
	batchedDir   string // sql/incremental/batched/*.sql
	immediateDir string // sql/incremental/immediate/*.sql

	// Current state (no locks needed - one runner per chain)
	latestBlockNumber uint64
	latestBlockTime   time.Time

	// Throttling for batched and immediate indexes
	lastBatchedRun   time.Time
	immediateLastRun map[string]time.Time
}

// NewIndexRunner creates an index runner for a specific chain
func NewIndexRunner(chainId uint32, conn driver.Conn, sqlBaseDir string) (*IndexRunner, error) {
	// Create watermark tables
	watermarkSQL, err := os.ReadFile(filepath.Join(filepath.Dir(sqlBaseDir), "pkg/indexer/watermarks.sql"))
	if err != nil {
		return nil, fmt.Errorf("failed to read watermarks.sql: %w", err)
	}

	statements := splitSQL(string(watermarkSQL))
	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := conn.Exec(context.Background(), stmt); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("failed to create watermark table: %w", err)
			}
		}
	}

	return &IndexRunner{
		chainId:          chainId,
		conn:             conn,
		metricsDir:       filepath.Join(sqlBaseDir, "sql/metrics"),
		batchedDir:       filepath.Join(sqlBaseDir, "sql/incremental/batched"),
		immediateDir:     filepath.Join(sqlBaseDir, "sql/incremental/immediate"),
		immediateLastRun: make(map[string]time.Time),
	}, nil
}

// OnBlock updates latest block state and triggers index processing
// Called by syncer when new blocks arrive
func (r *IndexRunner) OnBlock(blockNumber uint64, blockTime time.Time) error {
	r.latestBlockNumber = blockNumber
	r.latestBlockTime = blockTime

	// Process all index types
	// Order matters: metrics first (most important), then batched, then immediate
	if err := r.processMetrics(); err != nil {
		fmt.Printf("[IndexRunner Chain %d] Metrics error: %v\n", r.chainId, err)
	}

	if err := r.processBatched(); err != nil {
		fmt.Printf("[IndexRunner Chain %d] Batched error: %v\n", r.chainId, err)
	}

	if err := r.processImmediate(); err != nil {
		fmt.Printf("[IndexRunner Chain %d] Immediate error: %v\n", r.chainId, err)
	}

	return nil
}

// processMetrics handles time-based granular metrics
func (r *IndexRunner) processMetrics() error {
	files, err := os.ReadDir(r.metricsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No metrics directory yet
		}
		return fmt.Errorf("failed to read metrics directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") || file.Name() == "README.md" {
			continue
		}

		metricName := strings.TrimSuffix(file.Name(), ".sql")

		// Process for all granularities
		for _, granularity := range []string{"hour", "day", "week", "month"} {
			if err := r.processMetric(metricName, granularity); err != nil {
				fmt.Printf("[Metrics Chain %d] Error processing %s_%s: %v\n", r.chainId, metricName, granularity, err)
			}
		}
	}

	return nil
}

// processMetric processes a single metric for a given granularity
func (r *IndexRunner) processMetric(metricName, granularity string) error {
	watermarkKey := fmt.Sprintf("%s_%s", metricName, granularity)
	lastProcessed := r.getMetricWatermark(watermarkKey)

	// If never processed, start from earliest block
	isFirstRun := lastProcessed.IsZero()
	if isFirstRun {
		earliestBlock := r.getEarliestBlockTime()
		if earliestBlock.IsZero() {
			return nil // No data yet
		}
		// Set to one period before first data to ensure clean boundaries
		firstDataPeriod := toStartOfPeriod(earliestBlock, granularity)
		lastProcessed = firstDataPeriod.Add(-getPeriodDuration(granularity))
	}

	// Get complete periods to process
	periods := getPeriodsToProcess(lastProcessed, r.latestBlockTime, granularity)
	if len(periods) == 0 {
		// Save watermark on first run even if no complete periods yet
		if isFirstRun {
			r.setMetricWatermark(watermarkKey, lastProcessed)
		}
		return nil
	}

	// Read and execute SQL
	sqlPath := filepath.Join(r.metricsDir, metricName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	statements := splitSQL(string(sqlBytes))

	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Replace placeholders
		sql := r.prepareMetricSQL(stmt, granularity, periods)

		// Execute
		if err := r.conn.Exec(context.Background(), sql); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	// Update watermark
	return r.setMetricWatermark(watermarkKey, periods[len(periods)-1])
}

// processBatched handles block-number-based batched incrementals
// Throttled to run at most every 5 minutes
func (r *IndexRunner) processBatched() error {
	// Throttle: don't run more than once per 5 minutes
	if time.Since(r.lastBatchedRun) < 5*time.Minute {
		return nil
	}

	files, err := os.ReadDir(r.batchedDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No batched directory yet
		}
		return fmt.Errorf("failed to read batched directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		indexName := strings.TrimSuffix(file.Name(), ".sql")
		if err := r.processBatchedIndex(indexName); err != nil {
			fmt.Printf("[Batched Chain %d] Error processing %s: %v\n", r.chainId, indexName, err)
		}
	}

	r.lastBatchedRun = time.Now()
	return nil
}

// processBatchedIndex processes a single batched incremental index
func (r *IndexRunner) processBatchedIndex(indexName string) error {
	lastBlockNumber := r.getBatchedWatermark(indexName)

	// Nothing to process if we're caught up
	if lastBlockNumber >= r.latestBlockNumber {
		return nil
	}

	// Read SQL file
	sqlPath := filepath.Join(r.batchedDir, indexName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	statements := splitSQL(string(sqlBytes))

	// Get block time range for the block range
	firstBlockTime, lastBlockTime := r.getBlockTimeRange(lastBlockNumber+1, r.latestBlockNumber)

	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Replace placeholders for batched incrementals
		sql := stmt
		sql = strings.ReplaceAll(sql, "{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId))
		sql = strings.ReplaceAll(sql, "{first_period:DateTime}",
			fmt.Sprintf("toDateTime64('%s', 3)", firstBlockTime.Format("2006-01-02 15:04:05.000")))
		sql = strings.ReplaceAll(sql, "{last_period:DateTime}",
			fmt.Sprintf("toDateTime64('%s', 3)", lastBlockTime.Format("2006-01-02 15:04:05.000")))

		if err := r.conn.Exec(context.Background(), sql); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	// Update watermark (idempotent: run first, then save watermark)
	return r.setBatchedWatermark(indexName, r.latestBlockNumber)
}

// processImmediate handles immediate incrementals
// Throttled to run at most every 0.9 seconds per index
func (r *IndexRunner) processImmediate() error {
	files, err := os.ReadDir(r.immediateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No immediate directory yet
		}
		return fmt.Errorf("failed to read immediate directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		indexName := strings.TrimSuffix(file.Name(), ".sql")

		// Throttle per index: at least 0.9 seconds between runs
		lastRun, exists := r.immediateLastRun[indexName]
		if exists && time.Since(lastRun) < 900*time.Millisecond {
			continue
		}

		if err := r.processImmediateIndex(indexName); err != nil {
			fmt.Printf("[Immediate Chain %d] Error processing %s: %v\n", r.chainId, indexName, err)
		}

		r.immediateLastRun[indexName] = time.Now()
	}

	return nil
}

// processImmediateIndex processes a single immediate incremental index
func (r *IndexRunner) processImmediateIndex(indexName string) error {
	// Read SQL file
	sqlPath := filepath.Join(r.immediateDir, indexName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	statements := splitSQL(string(sqlBytes))

	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Replace placeholders - immediate indexes run on latest data
		sql := stmt
		sql = strings.ReplaceAll(sql, "{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId))

		if err := r.conn.Exec(context.Background(), sql); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	return nil
}

// prepareMetricSQL replaces placeholders in metric SQL
func (r *IndexRunner) prepareMetricSQL(stmt, granularity string, periods []time.Time) string {
	sql := stmt
	sql = strings.ReplaceAll(sql, "{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId))

	// Replace granularity patterns
	sql = strings.ReplaceAll(sql, "toStartOf{granularity}", fmt.Sprintf("toStartOf%s", capitalize(granularity)))
	sql = strings.ReplaceAll(sql, "_{granularity}", fmt.Sprintf("_%s", granularity))
	sql = strings.ReplaceAll(sql, "{granularity}", granularity)

	// Replace period placeholders
	firstPeriod := periods[0]
	lastPeriod := nextPeriod(periods[len(periods)-1], granularity)

	sql = strings.ReplaceAll(sql, "{first_period:DateTime}",
		fmt.Sprintf("toDateTime64('%s', 3)", firstPeriod.Format("2006-01-02 15:04:05.000")))
	sql = strings.ReplaceAll(sql, "{last_period:DateTime}",
		fmt.Sprintf("toDateTime64('%s', 3)", lastPeriod.Format("2006-01-02 15:04:05.000")))

	return sql
}

// getMetricWatermark retrieves the last processed period for a metric
func (r *IndexRunner) getMetricWatermark(metricName string) time.Time {
	var lastPeriod time.Time
	query := `SELECT last_period FROM metric_watermarks FINAL WHERE chain_id = ? AND metric_name = ?`
	row := r.conn.QueryRow(context.Background(), query, r.chainId, metricName)
	if err := row.Scan(&lastPeriod); err != nil {
		return time.Time{}
	}
	return lastPeriod
}

// setMetricWatermark updates the last processed period for a metric
func (r *IndexRunner) setMetricWatermark(metricName string, lastPeriod time.Time) error {
	query := `INSERT INTO metric_watermarks (chain_id, metric_name, last_period) VALUES (?, ?, ?)`
	return r.conn.Exec(context.Background(), query, r.chainId, metricName, lastPeriod)
}

// getBatchedWatermark retrieves the last processed block number for a batched index
func (r *IndexRunner) getBatchedWatermark(indexName string) uint64 {
	var lastBlockNumber uint64
	query := `SELECT last_block_number FROM batched_watermarks FINAL WHERE chain_id = ? AND index_name = ?`
	row := r.conn.QueryRow(context.Background(), query, r.chainId, indexName)
	if err := row.Scan(&lastBlockNumber); err != nil {
		return 0
	}
	return lastBlockNumber
}

// setBatchedWatermark updates the last processed block number for a batched index
func (r *IndexRunner) setBatchedWatermark(indexName string, lastBlockNumber uint64) error {
	query := `INSERT INTO batched_watermarks (chain_id, index_name, last_block_number) VALUES (?, ?, ?)`
	return r.conn.Exec(context.Background(), query, r.chainId, indexName, lastBlockNumber)
}

// getEarliestBlockTime returns the earliest block time for this chain
func (r *IndexRunner) getEarliestBlockTime() time.Time {
	var earliestTime time.Time
	query := `SELECT min(block_time) FROM raw_transactions WHERE chain_id = ?`
	row := r.conn.QueryRow(context.Background(), query, r.chainId)
	if err := row.Scan(&earliestTime); err != nil {
		return time.Time{}
	}
	return earliestTime
}

// getBlockTimeRange returns the time range for a block number range
func (r *IndexRunner) getBlockTimeRange(firstBlock, lastBlock uint64) (time.Time, time.Time) {
	var firstTime, lastTime time.Time

	query := `
	SELECT 
		min(block_time) as first_time,
		max(block_time) as last_time
	FROM raw_transactions
	WHERE chain_id = ? AND block_number >= ? AND block_number <= ?`

	row := r.conn.QueryRow(context.Background(), query, r.chainId, firstBlock, lastBlock)
	if err := row.Scan(&firstTime, &lastTime); err != nil {
		// Fallback to current block time
		return r.latestBlockTime, r.latestBlockTime
	}

	return firstTime, lastTime
}

// splitSQL splits SQL content by semicolons, removing comments
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

// capitalize returns string with first letter capitalized
func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
