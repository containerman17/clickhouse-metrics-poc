package main

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/platformvm/txs"
)

func main() {
	client := platformvm.NewClient("http://167.235.8.126:9650")

	txIdString := "23dqTMHK186m4Rzcn1ukJdmHy13nqido4LjTp5Kh9W6qBKaFib"
	txId, err := ids.FromString(txIdString)
	if err != nil {
		log.Fatalf("Failed to parse tx id: %v", err)
	}
	txBytes, err := client.GetTx(context.Background(), txId)
	if err != nil {
		log.Fatalf("Failed to get tx: %v", err)
	}

	tx, err := txs.Parse(txs.Codec, txBytes)
	if err != nil {
		log.Fatalf("Failed to unmarshal tx: %v", err)
	}

	// Access the unsigned transaction
	unsignedTx := tx.Unsigned

	// Check the type of the unsigned transaction
	fmt.Printf("Unsigned Tx Type: %s\n", reflect.TypeOf(unsignedTx))

	// Type switch to handle specific transaction types
	switch utx := unsignedTx.(type) {
	case *txs.ConvertSubnetToL1Tx:
		fmt.Printf("ConvertSubnetToL1Tx:\n")
		fmt.Printf("  Subnet ID: %s\n", utx.Subnet)
		fmt.Printf("  Chain ID: %s\n", utx.ChainID)
		fmt.Printf("  Address: %x\n", utx.Address)
		fmt.Printf("  Validators: %d\n", len(utx.Validators))
	case *txs.AddValidatorTx:
		fmt.Printf("AddValidatorTx\n")
	case *txs.BaseTx:
		fmt.Printf("BaseTx\n")
	case *txs.CreateSubnetTx:
		fmt.Printf("CreateSubnetTx\n")
		fmt.Printf("  Owner: %s\n", utx.Owner)
	default:
		panic("Unknown transaction type")
	}

	fmt.Printf("Tx ID: %s\n", tx.ID())
}
