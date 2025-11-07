package main

import (
	"clickhouse-metrics-poc/cmd"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func main() {
	_ = godotenv.Load()

	root := &cobra.Command{Use: "clickhouse-ingest"}

	wipeCmd := &cobra.Command{
		Use:   "wipe",
		Short: "Drop calculated tables (keeps raw_* and sync_watermark)",
		Run: func(command *cobra.Command, args []string) {
			all, _ := command.Flags().GetBool("all")
			cmd.RunWipe(all)
		},
	}
	wipeCmd.Flags().Bool("all", false, "Drop all tables including raw_* tables")

	root.AddCommand(
		&cobra.Command{
			Use:   "ingest",
			Short: "Start the continuous ingestion process",
			Run:   func(command *cobra.Command, args []string) { cmd.RunIngest() },
		},
		&cobra.Command{
			Use:   "cache",
			Short: "Fill RPC cache at max speed (no ClickHouse)",
			Run:   func(command *cobra.Command, args []string) { cmd.RunCache() },
		},
		&cobra.Command{
			Use:   "size",
			Short: "Show ClickHouse table sizes and disk usage",
			Run:   func(command *cobra.Command, args []string) { cmd.RunSize() },
		},
		wipeCmd,
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
