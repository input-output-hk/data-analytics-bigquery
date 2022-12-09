/*  Cardano on BigQuery

    example: simple query
*/


/*
authenticate: `gcloud auth application-default login`
prepare: `go mod tidy -v`
compile: `go build`
run: `go run . --epoch_no 321`
 or: `./simple_query -epoch_no 321`
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func main() {

	epoch_no := flag.Int("epoch_no", 251, "Indicate the epoch number for which you want to query its last block.")
	flag.Parse()
	if *epoch_no < 0 { log.Fatal("wrong epoch number specified.") }

	ctx := context.Background()

	// the GCP/BigQuery project id
	projectID := "iog-data-analytics"

	bqclient, err := bigquery.NewClient(ctx, projectID)
	if err != nil { log.Fatalf("bigquery.NewClient: %v", err) }
	defer bqclient.Close()

	rows, err := runQuery(ctx, bqclient, *epoch_no)
	if err != nil { log.Fatalf("run query: %v", err) }

	if err := printResults(rows); err != nil { log.Fatalf("print results: %v",err) }
}

func runQuery(ctx context.Context, client *bigquery.Client, p_epoch_no int) (*bigquery.RowIterator, error) {
	query := client.Query(
		`SELECT epoch_no, slot_no, block_time, block_size, tx_count,
		        sum_tx_fee, script_count, sum_script_size, pool_hash
		 FROM ` + "`cardano_mainnet.block`" + `
		 WHERE epoch_no = ` + fmt.Sprintf("%d",p_epoch_no) + `
		 ORDER BY slot_no DESC LIMIT 1;`)
	return query.Read(ctx)
}

type Block struct {
	EpochNo     int64  `bigquery:"epoch_no"`
	SlotNo      int64  `bigquery:"slot_no"`
	BlockTime   bigquery.NullDateTime `bigquery:"block_time"`
	BlockSize   int64  `bigquery:"block_size"`
	TxCount     int64  `bigquery:"tx_count"`
	SumTxFee    int64  `bigquery:"sum_tx_fee"`
	ScriptCount int64  `bigquery:"script_count"`
	ScriptSize  int64  `bigquery:"sum_script_size"`
	PoolHash    string  `bigquery:"pool_hash"`
}
func block2String(b *Block) string {
	return fmt.Sprintf("epoch: %d, slot: %d, timestamp: %v, block sz: %d, tx count: %d, fees: %d, scripts: %d/%d bytes, pool: %v",
					   b.EpochNo, b.SlotNo, b.BlockTime, b.BlockSize, b.TxCount, b.SumTxFee, b.ScriptCount, b.ScriptSize, b.PoolHash)
}

func printResults(iter *bigquery.RowIterator) error {
	for {
		var row Block
		err := iter.Next(&row)
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error iterating through results: %w", err)
		}

		fmt.Println(block2String(&row))
	}
}
