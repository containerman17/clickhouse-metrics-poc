package cmd

import (
	"clickhouse-metrics-poc/pkg/chwrapper"
	"clickhouse-metrics-poc/pkg/ingest/cache"
	"clickhouse-metrics-poc/pkg/ingest/syncer"
	"encoding/json"
	"log"
	"os"
	"sync"
)

type ChainConfig struct {
	ChainID        uint32 `json:"chainID"`
	RpcURL         string `json:"rpcURL"`
	StartBlock     int64  `json:"startBlock,omitempty"`
	MaxConcurrency int    `json:"maxConcurrency,omitempty"`
	FetchBatchSize int    `json:"fetchBatchSize,omitempty"`
}

func RunIngest() {
	log.Println("Starting ingest...")
	// Load configuration
	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read config.json: %v", err)
	}

	var configs []ChainConfig
	if err := json.Unmarshal(configData, &configs); err != nil {
		log.Fatalf("Failed to parse config.json: %v", err)
	}

	if len(configs) == 0 {
		log.Fatal("No chain configurations found in config.json")
	}

	// Connect to ClickHouse
	conn, err := chwrapper.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	err = chwrapper.CreateTables(conn)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	var wg sync.WaitGroup

	// Start a syncer for each chain
	for _, cfg := range configs {
		// Create cache
		cacheInstance, err := cache.New("./rpc_cache", cfg.ChainID)
		if err != nil {
			log.Fatalf("Failed to create cache for chain %d: %v", cfg.ChainID, err)
		}
		defer cacheInstance.Close()

		// TODO: delete this after testing
		// go func(c *cache.Cache, chainID uint32) {
		// 	log.Printf("Starting background compaction for chain %d...", chainID)
		// 	if err := c.Compact(); err != nil {
		// 		log.Printf("Background compaction failed for chain %d: %v", chainID, err)
		// 	} else {
		// 		log.Printf("Background compaction completed for chain %d", chainID)
		// 	}
		// }(cacheInstance, cfg.ChainID)

		// Create syncer
		chainSyncer, err := syncer.NewChainSyncer(syncer.Config{
			ChainID:        cfg.ChainID,
			RpcURL:         cfg.RpcURL,
			StartBlock:     cfg.StartBlock,
			MaxConcurrency: cfg.MaxConcurrency,
			CHConn:         conn,
			Cache:          cacheInstance,
			FetchBatchSize: cfg.FetchBatchSize,
		})
		if err != nil {
			log.Fatalf("Failed to create syncer for chain %d: %v", cfg.ChainID, err)
		}

		wg.Add(1)
		go func(cs *syncer.ChainSyncer, chainID uint32) {
			defer wg.Done()
			if err := cs.Start(); err != nil {
				log.Printf("Failed to start syncer for chain %d: %v", chainID, err)
			}
			cs.Wait()
		}(chainSyncer, cfg.ChainID)

		log.Printf("Started syncer for chain %d", cfg.ChainID)
	}

	wg.Wait()
}
