package metrics

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MetricsRunner is an ultra-simple metric processor using watermarks
type MetricsRunner struct {
	conn        driver.Conn
	sqlDir      string
	chainId     uint32
	latestBlock time.Time
}

// NewMetricsRunner creates a simple metrics runner
func NewMetricsRunner(conn driver.Conn, sqlDir string) (*MetricsRunner, error) {
	// Create watermark table if not exists
	watermarkSQL := `
	CREATE TABLE IF NOT EXISTS metric_watermarks (
		chain_id UInt32,
		metric_name String,
		last_period DateTime64(3, 'UTC'),
		updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (chain_id, metric_name)`

	if err := conn.Exec(context.Background(), watermarkSQL); err != nil {
		return nil, fmt.Errorf("failed to create watermark table: %w", err)
	}

	return &MetricsRunner{
		conn:    conn,
		sqlDir:  sqlDir,
		chainId: 1, // default chain
	}, nil
}

// OnBlock updates latest block time and processes metrics
func (r *MetricsRunner) OnBlock(blockTime time.Time, chainId uint32) error {
	r.chainId = chainId
	r.latestBlock = blockTime

	// Process all metrics
	return r.ProcessAllMetrics()
}

// ProcessAllMetrics runs all metric files in the sql/metrics directory
func (r *MetricsRunner) ProcessAllMetrics() error {
	files, err := os.ReadDir(r.sqlDir)
	if err != nil {
		return fmt.Errorf("failed to read metrics directory: %w", err)
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		metricName := strings.TrimSuffix(file.Name(), ".sql")

		// Process for all granularities
		for _, granularity := range []string{"hour", "day", "week", "month"} {
			if err := r.ProcessMetric(metricName, granularity); err != nil {
				// Log error but continue with other metrics
				fmt.Printf("[Metrics] Error processing %s_%s: %v\n", metricName, granularity, err)
			}
		}
	}

	return nil
}

// ProcessMetric processes a single metric for a given granularity
func (r *MetricsRunner) ProcessMetric(metricName, granularity string) error {
	// Get watermark
	watermarkKey := fmt.Sprintf("%s_%s", metricName, granularity)
	lastProcessed := r.getWatermark(watermarkKey)

	// If never processed, get the earliest block time
	isFirstRun := lastProcessed.IsZero()
	if isFirstRun {
		earliestBlock := r.getEarliestBlockTime()
		if earliestBlock.IsZero() {
			return nil
		}
		// Set watermark to one period before the first data to ensure we capture it
		// This ensures clean period boundaries (e.g., 13:42:00 not 13:42:59)
		firstDataPeriod := toStartOfPeriod(earliestBlock, granularity)
		// Go back one period - nextPeriod(lastProcessed) will then give us firstDataPeriod
		lastProcessed = firstDataPeriod.Add(-getPeriodDuration(granularity))
		fmt.Printf("[Metrics] %s - Starting from earliest data: %s\n", watermarkKey, earliestBlock.Format(time.RFC3339))
	}

	// Calculate periods to process
	periods := getPeriodsToProcess(lastProcessed, r.latestBlock, granularity)
	if len(periods) == 0 {
		// Save watermark on first run even if no complete periods yet
		// This prevents re-checking from scratch every block
		if isFirstRun {
			r.setWatermark(watermarkKey, lastProcessed)
		}
		return nil // Nothing to process
	}

	// fmt.Printf("[Metrics] %s - Processing %d periods\n", watermarkKey, len(periods))

	// Read SQL file
	sqlPath := filepath.Join(r.sqlDir, metricName+".sql")
	sqlBytes, err := os.ReadFile(sqlPath)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	// Split by semicolon and execute each statement
	statements := splitSQL(string(sqlBytes))

	for _, stmt := range statements {
		// Skip empty statements
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Replace placeholders
		sql := stmt
		sql = strings.ReplaceAll(sql, "{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId))

		// Replace specific patterns BEFORE generic {granularity} replacement
		sql = strings.ReplaceAll(sql, "toStartOf{granularity}", fmt.Sprintf("toStartOf%s", capitalize(granularity)))
		sql = strings.ReplaceAll(sql, "_{granularity}", fmt.Sprintf("_%s", granularity))

		// Replace any remaining {granularity} placeholders
		sql = strings.ReplaceAll(sql, "{granularity}", granularity)

		// Replace period placeholders
		firstPeriod := periods[0]
		lastPeriod := nextPeriod(periods[len(periods)-1], granularity) // exclusive end

		sql = strings.ReplaceAll(sql, "{first_period:DateTime}",
			fmt.Sprintf("toDateTime64('%s', 3)", firstPeriod.Format("2006-01-02 15:04:05.000")))
		sql = strings.ReplaceAll(sql, "{last_period:DateTime}",
			fmt.Sprintf("toDateTime64('%s', 3)", lastPeriod.Format("2006-01-02 15:04:05.000")))

		// Execute statement
		if err := r.conn.Exec(context.Background(), sql); err != nil {
			// Check if it's a CREATE TABLE that already exists (not an error)
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("failed to execute SQL: %w", err)
			}
		}
	}

	// Update watermark after successful execution
	return r.setWatermark(watermarkKey, periods[len(periods)-1])
}

// getWatermark retrieves the last processed period for a metric
func (r *MetricsRunner) getWatermark(metricName string) time.Time {
	ctx := context.Background()
	var lastPeriod time.Time

	query := `
	SELECT last_period 
	FROM metric_watermarks FINAL
	WHERE chain_id = ? AND metric_name = ?`

	row := r.conn.QueryRow(ctx, query, r.chainId, metricName)
	if err := row.Scan(&lastPeriod); err != nil {
		// No watermark found, return zero time
		return time.Time{}
	}

	return lastPeriod
}

// setWatermark updates the last processed period for a metric
func (r *MetricsRunner) setWatermark(metricName string, lastPeriod time.Time) error {
	ctx := context.Background()

	query := `
	INSERT INTO metric_watermarks (chain_id, metric_name, last_period)
	VALUES (?, ?, ?)`

	return r.conn.Exec(ctx, query, r.chainId, metricName, lastPeriod)
}

// getEarliestBlockTime returns the earliest block time in the database
func (r *MetricsRunner) getEarliestBlockTime() time.Time {
	ctx := context.Background()
	var earliestTime time.Time

	query := `
	SELECT min(block_time) 
	FROM raw_txs 
	WHERE chain_id = ?`

	row := r.conn.QueryRow(ctx, query, r.chainId)
	if err := row.Scan(&earliestTime); err != nil {
		// No data yet
		return time.Time{}
	}

	return earliestTime
}

// splitSQL splits SQL content by semicolons, removing comments
func splitSQL(content string) []string {
	lines := strings.Split(content, "\n")
	var cleanLines []string

	for _, line := range lines {
		// Remove comments
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cleanLines = append(cleanLines, line)
	}

	// Join and split by semicolon
	cleaned := strings.Join(cleanLines, "\n")
	statements := strings.Split(cleaned, ";")

	// Filter empty statements
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

// Simple process loop for continuous processing
func (r *MetricsRunner) Start() {
	go func() {
		for {
			if !r.latestBlock.IsZero() {
				r.ProcessAllMetrics()
			}
			time.Sleep(5 * time.Second)
		}
	}()
}
