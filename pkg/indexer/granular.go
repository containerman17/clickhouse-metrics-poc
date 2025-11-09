package indexer

import (
	"fmt"
	"time"
)

var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// processGranularMetrics checks and runs all granular metrics
func (r *IndexRunner) processGranularMetrics() {
	for _, metricFile := range r.granularMetrics {
		for _, granularity := range []string{"hour", "day", "week", "month"} {
			// Use just the metric filename for indexer name, granularity tracked separately
			indexerName := fmt.Sprintf("metrics/%s", metricFile)

			watermark := r.getWatermarkWithGranularity(indexerName, granularity)

			// Initialize to epoch if never run
			if watermark.LastPeriod.IsZero() {
				watermark.LastPeriod = epoch
			}

			// Calculate periods to process
			periods := getPeriodsToProcess(watermark.LastPeriod, r.latestBlockTime, granularity)
			if len(periods) == 0 {
				continue
			}

			// Run metric
			start := time.Now()
			if err := r.runGranularMetric(metricFile, granularity, periods); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to run %s (%s): %v\n", r.chainId, indexerName, granularity, err)
				panic(err)
			}
			elapsed := time.Since(start)
			fmt.Printf("[Chain %d] %s (%s) - processed %d periods - time taken: %s\n",
				r.chainId, indexerName, granularity, len(periods), elapsed)

			// Update watermark
			watermark.LastPeriod = periods[len(periods)-1]
			if err := r.saveWatermarkWithGranularity(indexerName, granularity, watermark); err != nil {
				fmt.Printf("[Chain %d] FATAL: Failed to save watermark for %s (%s): %v\n", r.chainId, indexerName, granularity, err)
				panic(err)
			}
		}
	}
}

// runGranularMetric executes a single granular metric for given periods
func (r *IndexRunner) runGranularMetric(metricFile string, granularity string, periods []time.Time) error {
	firstPeriod := periods[0]
	lastPeriod := nextPeriod(periods[len(periods)-1], granularity) // exclusive end

	params := []struct{ key, value string }{
		{"{chain_id:UInt32}", fmt.Sprintf("%d", r.chainId)},
		{"{first_period:DateTime}", fmt.Sprintf("toDateTime64('%s', 3)", firstPeriod.Format("2006-01-02 15:04:05.000"))},
		{"{last_period:DateTime}", fmt.Sprintf("toDateTime64('%s', 3)", lastPeriod.Format("2006-01-02 15:04:05.000"))},
		{"{granularity}", granularity},
		{"{granularityCamelCase}", capitalize(granularity)},
	}

	filename := fmt.Sprintf("metrics/%s.sql", metricFile)
	return executeSQLFile(r.conn, r.sqlDir, filename, params)
}
