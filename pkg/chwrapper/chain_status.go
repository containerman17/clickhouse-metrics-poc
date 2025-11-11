package chwrapper

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// UpsertChainStatus inserts or updates chain status with name and initial metadata
func UpsertChainStatus(conn driver.Conn, chainID uint32, name string, lastBlockOnChain uint64) error {
	ctx := context.Background()

	query := `
	INSERT INTO chain_status (chain_id, name, last_updated, last_block_on_chain) 
	VALUES (?, ?, ?, ?)`

	now := time.Now().UTC()
	if err := conn.Exec(ctx, query, chainID, name, now, lastBlockOnChain); err != nil {
		return fmt.Errorf("failed to upsert chain status: %w", err)
	}

	return nil
}

// UpdateLatestBlock updates the last_block_on_chain and last_updated fields
func UpdateLatestBlock(conn driver.Conn, chainID uint32, name string, lastBlockOnChain uint64) error {
	ctx := context.Background()

	query := `
	INSERT INTO chain_status (chain_id, name, last_updated, last_block_on_chain) 
	VALUES (?, ?, ?, ?)`

	now := time.Now().UTC()
	if err := conn.Exec(ctx, query, chainID, name, now, lastBlockOnChain); err != nil {
		return fmt.Errorf("failed to update latest block: %w", err)
	}

	return nil
}
