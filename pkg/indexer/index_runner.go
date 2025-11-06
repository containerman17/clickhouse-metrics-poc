package indexer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var metricGranularities = []string{"hour", "day", "week", "month"}

const (
	batchedMinInterval   = 5 * time.Minute
	immediateMinInterval = 900 * time.Millisecond
)

// Options configures the index runner file locations.
type Options struct {
	MetricsDir   string
	BatchedDir   string
	ImmediateDir string
}

// IndexRunner executes metric and incremental index SQL for a single chain.
type IndexRunner struct {
	conn          driver.Conn
	chainID       uint32
	metricsDir    string
	batchedDir    string
	immediateDir  string
	lastBatched   map[string]time.Time
	lastImmediate map[string]time.Time
}

// NewIndexRunner creates a chain-scoped index runner.
func NewIndexRunner(conn driver.Conn, chainID uint32, opts Options) (*IndexRunner, error) {
	if conn == nil {
		return nil, fmt.Errorf("nil clickhouse connection")
	}

	if opts.MetricsDir == "" {
		opts.MetricsDir = filepath.Join("sql", "metrics")
	}
	if opts.BatchedDir == "" {
		opts.BatchedDir = filepath.Join("sql", "incremental", "batched")
	}
	if opts.ImmediateDir == "" {
		opts.ImmediateDir = filepath.Join("sql", "incremental", "immediate")
	}

	metricsDir, err := normalizeDir(opts.MetricsDir)
	if err != nil {
		return nil, fmt.Errorf("metrics dir validation failed: %w", err)
	}
	batchedDir, err := normalizeDir(opts.BatchedDir)
	if err != nil {
		return nil, fmt.Errorf("batched dir validation failed: %w", err)
	}
	immediateDir, err := normalizeDir(opts.ImmediateDir)
	if err != nil {
		return nil, fmt.Errorf("immediate dir validation failed: %w", err)
	}

	if err := ensureTables(conn); err != nil {
		return nil, err
	}

	return &IndexRunner{
		conn:          conn,
		chainID:       chainID,
		metricsDir:    metricsDir,
		batchedDir:    batchedDir,
		immediateDir:  immediateDir,
		lastBatched:   make(map[string]time.Time),
		lastImmediate: make(map[string]time.Time),
	}, nil
}

// HandleBatch processes all index classes after a batch of blocks is written.
func (r *IndexRunner) HandleBatch(latestBlockTime time.Time, latestBlockNumber uint64) error {
	if r.metricsDir != "" {
		if err := r.processMetrics(latestBlockTime); err != nil {
			return err
		}
	}

	if latestBlockNumber == 0 {
		return nil
	}

	if r.immediateDir != "" {
		if err := r.processImmediateIncremental(latestBlockNumber); err != nil {
			return err
		}
	}

	if r.batchedDir != "" {
		if err := r.processBatchedIncremental(latestBlockNumber); err != nil {
			return err
		}
	}

	return nil
}

func (r *IndexRunner) processMetrics(latestBlockTime time.Time) error {
	if latestBlockTime.IsZero() {
		return nil
	}

	entries, err := os.ReadDir(r.metricsDir)
	if err != nil {
		return fmt.Errorf("read metrics dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		metricName := strings.TrimSuffix(entry.Name(), ".sql")
		for _, granularity := range metricGranularities {
			if err := r.processMetric(metricName, granularity, latestBlockTime); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *IndexRunner) processMetric(metricName, granularity string, latestBlockTime time.Time) error {
	watermarkKey := fmt.Sprintf("%s_%s", metricName, granularity)
	lastProcessed, err := r.getMetricWatermark(watermarkKey)
	if err != nil {
		return fmt.Errorf("get metric watermark %s: %w", watermarkKey, err)
	}

	isFirstRun := lastProcessed.IsZero()
	if isFirstRun {
		earliest, err := r.getEarliestBlockTime()
		if err != nil {
			return fmt.Errorf("get earliest block: %w", err)
		}
		if earliest.IsZero() {
			return nil
		}

		firstPeriod := toStartOfPeriod(earliest, granularity)
		lastProcessed = firstPeriod.Add(-getPeriodDuration(granularity))
	}

	periods := getPeriodsToProcess(lastProcessed, latestBlockTime, granularity)
	if len(periods) == 0 {
		if isFirstRun {
			return r.setMetricWatermark(watermarkKey, lastProcessed)
		}
		return nil
	}

	statements, err := r.loadStatements(filepath.Join(r.metricsDir, metricName+".sql"))
	if err != nil {
		return fmt.Errorf("load metric %s: %w", metricName, err)
	}

	firstPeriod := periods[0]
	lastPeriodExclusive := nextPeriod(periods[len(periods)-1], granularity)

	for _, stmt := range statements {
		sql := replaceCommonPlaceholders(stmt, r.chainID)
		sql = strings.ReplaceAll(sql, "toStartOf{granularity}", fmt.Sprintf("toStartOf%s", capitalize(granularity)))
		sql = strings.ReplaceAll(sql, "_{granularity}", fmt.Sprintf("_%s", granularity))
		sql = strings.ReplaceAll(sql, "{granularity}", granularity)
		sql = strings.ReplaceAll(sql, "{first_period:DateTime}", toDateTime64Literal(firstPeriod))
		sql = strings.ReplaceAll(sql, "{last_period:DateTime}", toDateTime64Literal(lastPeriodExclusive))

		if err := r.execSQL(sql); err != nil {
			if !isTableExistsError(err) {
				return fmt.Errorf("metric %s_%s exec: %w", metricName, granularity, err)
			}
		}
	}

	return r.setMetricWatermark(watermarkKey, periods[len(periods)-1])
}

func (r *IndexRunner) processImmediateIncremental(latestBlock uint64) error {
	entries, err := os.ReadDir(r.immediateDir)
	if err != nil {
		return fmt.Errorf("read immediate dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".sql")
		lastBlock, err := r.getIncrementalWatermark("immediate", name)
		if err != nil {
			return fmt.Errorf("get immediate watermark %s: %w", name, err)
		}

		startBlock := lastBlock + 1
		if startBlock > latestBlock {
			continue
		}

		if lastRun := r.lastImmediate[name]; !lastRun.IsZero() {
			elapsed := time.Since(lastRun)
			if elapsed < immediateMinInterval {
				time.Sleep(immediateMinInterval - elapsed)
			}
		}

		if err := r.runIncrementalFile(filepath.Join(r.immediateDir, entry.Name()), startBlock, latestBlock); err != nil {
			return fmt.Errorf("run immediate %s: %w", name, err)
		}

		if err := r.setIncrementalWatermark("immediate", name, latestBlock); err != nil {
			return fmt.Errorf("set immediate watermark %s: %w", name, err)
		}

		r.lastImmediate[name] = time.Now()
	}

	return nil
}

func (r *IndexRunner) processBatchedIncremental(latestBlock uint64) error {
	entries, err := os.ReadDir(r.batchedDir)
	if err != nil {
		return fmt.Errorf("read batched dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".sql")
		if lastRun := r.lastBatched[name]; !lastRun.IsZero() && time.Since(lastRun) < batchedMinInterval {
			continue
		}

		lastBlock, err := r.getIncrementalWatermark("batched", name)
		if err != nil {
			return fmt.Errorf("get batched watermark %s: %w", name, err)
		}

		startBlock := lastBlock + 1
		if startBlock > latestBlock {
			continue
		}

		if err := r.runIncrementalFile(filepath.Join(r.batchedDir, entry.Name()), startBlock, latestBlock); err != nil {
			return fmt.Errorf("run batched %s: %w", name, err)
		}

		if err := r.setIncrementalWatermark("batched", name, latestBlock); err != nil {
			return fmt.Errorf("set batched watermark %s: %w", name, err)
		}

		r.lastBatched[name] = time.Now()
	}

	return nil
}

func (r *IndexRunner) runIncrementalFile(path string, firstBlock, lastBlock uint64) error {
	statements, err := r.loadStatements(path)
	if err != nil {
		return err
	}

	for _, stmt := range statements {
		sql := replaceCommonPlaceholders(stmt, r.chainID)
		sql = replaceBlockPlaceholder(sql, firstBlock, lastBlock)

		if err := r.execSQL(sql); err != nil {
			if !isTableExistsError(err) {
				return err
			}
		}
	}

	return nil
}

func (r *IndexRunner) loadStatements(path string) ([]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sql %s: %w", path, err)
	}
	return splitSQL(string(content)), nil
}

func (r *IndexRunner) execSQL(statement string) error {
	ctx := context.Background()
	return r.conn.Exec(ctx, statement)
}

func (r *IndexRunner) getMetricWatermark(metricName string) (time.Time, error) {
	ctx := context.Background()
	var lastPeriod time.Time

	query := `
	SELECT last_period
	FROM metric_watermarks FINAL
	WHERE chain_id = ? AND metric_name = ?`

	row := r.conn.QueryRow(ctx, query, r.chainID, metricName)
	if err := row.Scan(&lastPeriod); err != nil {
		return time.Time{}, nil
	}

	return lastPeriod, nil
}

func (r *IndexRunner) setMetricWatermark(metricName string, lastPeriod time.Time) error {
	ctx := context.Background()
	query := `
	INSERT INTO metric_watermarks (chain_id, metric_name, last_period)
	VALUES (?, ?, ?)`

	return r.conn.Exec(ctx, query, r.chainID, metricName, lastPeriod)
}

func (r *IndexRunner) getEarliestBlockTime() (time.Time, error) {
	ctx := context.Background()
	var earliest time.Time

	query := `
	SELECT min(block_time)
	FROM raw_transactions
	WHERE chain_id = ?`

	row := r.conn.QueryRow(ctx, query, r.chainID)
	if err := row.Scan(&earliest); err != nil {
		return time.Time{}, nil
	}

	return earliest, nil
}

func (r *IndexRunner) getIncrementalWatermark(indexType, indexName string) (uint64, error) {
	ctx := context.Background()
	var lastBlock uint64

	query := `
	SELECT last_block
	FROM incremental_watermarks FINAL
	WHERE chain_id = ? AND index_type = ? AND index_name = ?`

	row := r.conn.QueryRow(ctx, query, r.chainID, indexType, indexName)
	if err := row.Scan(&lastBlock); err != nil {
		return 0, nil
	}

	return lastBlock, nil
}

func (r *IndexRunner) setIncrementalWatermark(indexType, indexName string, lastBlock uint64) error {
	ctx := context.Background()
	query := `
	INSERT INTO incremental_watermarks (chain_id, index_type, index_name, last_block)
	VALUES (?, ?, ?, ?)`

	return r.conn.Exec(ctx, query, r.chainID, indexType, indexName, lastBlock)
}

func ensureTables(conn driver.Conn) error {
	ctx := context.Background()

	metricSQL := `
	CREATE TABLE IF NOT EXISTS metric_watermarks (
		chain_id UInt32,
		metric_name String,
		last_period DateTime64(3, 'UTC'),
		updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (chain_id, metric_name)`

	if err := conn.Exec(ctx, metricSQL); err != nil {
		return fmt.Errorf("create metric_watermarks: %w", err)
	}

	incrementalSQL := `
	CREATE TABLE IF NOT EXISTS incremental_watermarks (
		chain_id UInt32,
		index_type String,
		index_name String,
		last_block UInt64,
		updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY (chain_id, index_type, index_name)`

	if err := conn.Exec(ctx, incrementalSQL); err != nil {
		return fmt.Errorf("create incremental_watermarks: %w", err)
	}

	return nil
}

func normalizeDir(path string) (string, error) {
	if path == "" {
		return "", nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		return "", err
	}

	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", path)
	}

	return path, nil
}

func replaceCommonPlaceholders(sql string, chainID uint32) string {
	sql = strings.ReplaceAll(sql, "{chain_id:UInt32}", fmt.Sprintf("%d", chainID))
	return sql
}

func replaceBlockPlaceholder(sql string, firstBlock, lastBlock uint64) string {
	first := fmt.Sprintf("%d", firstBlock)
	last := fmt.Sprintf("%d", lastBlock)
	lastExclusive := fmt.Sprintf("%d", lastBlock+1)

	replacements := map[string]string{
		"{first_block:UInt32}":          first,
		"{first_block:UInt64}":          first,
		"{last_block:UInt32}":           last,
		"{last_block:UInt64}":           last,
		"{last_block_exclusive:UInt32}": lastExclusive,
		"{last_block_exclusive:UInt64}": lastExclusive,
	}

	for placeholder, value := range replacements {
		sql = strings.ReplaceAll(sql, placeholder, value)
	}

	return sql
}

func toDateTime64Literal(t time.Time) string {
	return fmt.Sprintf("toDateTime64('%s', 3)", t.UTC().Format("2006-01-02 15:04:05.000"))
}

func isTableExistsError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "already exists")
}

// splitSQL removes line comments and splits by semicolons.
func splitSQL(content string) []string {
	lines := strings.Split(content, "\n")
	var cleanLines []string

	for _, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cleanLines = append(cleanLines, line)
	}

	joined := strings.Join(cleanLines, "\n")
	parts := strings.Split(joined, ";")
	var statements []string
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			statements = append(statements, trimmed)
		}
	}

	return statements
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
