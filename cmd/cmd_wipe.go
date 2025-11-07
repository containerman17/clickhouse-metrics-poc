package cmd

import (
	"context"
	"fmt"
	"log"

	"clickhouse-metrics-poc/pkg/chwrapper"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func RunWipe(all bool) {
	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	if err := wipeCalculatedTables(conn, all); err != nil {
		log.Fatalf("Failed to wipe tables: %v", err)
	}

	if all {
		fmt.Println("All tables dropped successfully")
	} else {
		fmt.Println("Calculated tables dropped successfully")
	}
}

func wipeCalculatedTables(conn driver.Conn, all bool) error {
	ctx := context.Background()

	query := `
		SELECT name, database 
		FROM system.tables 
		WHERE database = currentDatabase()
		AND engine != 'System'
		ORDER BY engine = 'MaterializedView' DESC, name
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	keepTables := map[string]bool{}

	if !all {
		keepTables["raw_blocks"] = true
		keepTables["raw_txs"] = true
		keepTables["raw_traces"] = true
		keepTables["raw_logs"] = true
		keepTables["sync_watermark"] = true
	}

	var tables []struct {
		name     string
		database string
	}

	for rows.Next() {
		var name, database string
		if err := rows.Scan(&name, &database); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if !keepTables[name] {
			tables = append(tables, struct {
				name     string
				database string
			}{name: name, database: database})
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	if len(tables) == 0 {
		fmt.Println("No calculated tables found to drop")
		return nil
	}

	fmt.Printf("Found %d calculated tables to drop\n", len(tables))

	for _, table := range tables {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", table.database, table.name)
		fmt.Printf("Dropping %s.%s...\n", table.database, table.name)

		if err := conn.Exec(ctx, dropQuery); err != nil {
			return fmt.Errorf("failed to drop %s.%s: %w", table.database, table.name, err)
		}
	}

	return nil
}
