#!/bin/bash

if [ $# -lt 2 ]; then
	echo "$0 bq_epoch_no pg_epoch_no"
	exit 1
fi

set -e

source config.pg
source functions.sh

TNAME="epoch_param"

BQ_EPOCH_NO=$1
PG_EPOCH_NO=$2

if [ "$BQ_EPOCH_NO" -eq "$PG_EPOCH_NO" ]; then
	echo "Last epoch in BigQuery is $BQ_EPOCH_NO. Nothing to do."
	exit 1
elif [ "$BQ_EPOCH_NO" -gt "$PG_EPOCH_NO" ]; then
  echo "Last epoch in BigQuery is $BQ_EPOCH_NO > $PG_EPOCH_NO (epoch in Postgres)"
  exit 1
else 
  EPOCH_NO=$(( BQ_EPOCH_NO + 1 ))
  echo "BQ @ ${EPOCH_NO}"
  echo "   loading for epoch $EPOCH_NO"
fi

CSVNAME="update_epoch_param-q1"
if [ -e "${CSVNAME}.csv" ]; then rm -f "${CSVNAME}.csv"; fi
SCHEMA="epoch_param"
DATASETID="iog-data-analytics:db_sync"

## 1 insert epoch into table: tmp_epoch_param_1
TMPTBL="tmp_epoch_param_1"
Q="
    SELECT epoch_no, min_fee_a, min_fee_b,
         max_block_size, max_tx_size, max_bh_size,
         key_deposit, pool_deposit, max_epoch,
         optimal_pool_count, influence, 
         monetary_expand_rate, treasury_growth_rate,
         decentralisation,
         extra_entropy,
         protocol_major, protocol_minor,
         min_utxo_value, min_pool_cost,
         nonce,
         coins_per_utxo_size, cost_model,
         price_mem, price_step,
         max_tx_ex_mem, max_tx_ex_steps,
         max_block_ex_mem, max_block_ex_steps,
         max_val_size,
         collateral_percent, max_collateral_inputs
  FROM analytics.vw_bq_epoch_param
  WHERE epoch_no = ${EPOCH_NO}"
NREAD=$(pg_query_to_csv "${Q}" "$CSVNAME")

TARGETTBL="iog-data-analytics.cardano_mainnet.${TNAME}"

#DRYRUN="--dry_run"
DRYRUN=

if [ -z "${NREAD}" -o $NREAD -lt 0 ]
then 
  echo "Q: returned ${NREAD}."; 
  exit 1;
elif [ $NREAD -eq 0 ]
then
  echo "Q: returned ${NREAD}. Updating last index."; 
  Q="
   -- update the last index table
   UPDATE db_sync.last_index set last_epoch_no=${EPOCH_NO} WHERE tablename='${TARGETTBL}';"
  ${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_${TNAME}-query.err > logs/update_${TNAME}-query.out
  echo "index updated to epoch: ${EPOCH_NO}"
  exit 0;
fi
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "$DATASETID"

# run the transaction
SRCDATASET="iog-data-analytics.db_sync"
Q="
   BEGIN TRANSACTION;
   -- 1 insert new epoch
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${SRCDATASET}.${TMPTBL};
   -- 2 update the last index table
   UPDATE db_sync.last_index set last_epoch_no=${EPOCH_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_epoch_param-query.err > logs/update_epoch_param-query.out

echo
echo "values inserted successfully for epoch: ${EPOCH_NO}"
echo "all done."
