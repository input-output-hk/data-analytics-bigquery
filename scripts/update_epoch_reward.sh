#!/bin/bash

if [ $# -lt 2 ]; then
	echo "$0 bq_epoch_no pg_epoch_no"
	exit 1
fi

set -e

source config.pg
source functions.sh

TNAME="reward"

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

CSVNAME="update_${TNAME}-q1"
if [ -e "${CSVNAME}.csv" ]; then rm -f "${CSVNAME}.csv"; fi
SCHEMA="${TNAME}"
DATASETID="iog-data-analytics:db_sync"

## 1 insert epoch into tmp table
TMPTBL="tmp_${TNAME}_1"
Q="
  SELECT epoch_no, stake_addr_hash, type, amount, earned_epoch, pool_hash
  FROM analytics.vw_bq_reward
  WHERE epoch_no = ${EPOCH_NO}"
  
NREAD=$(pg_query_to_csv "${Q}" "$CSVNAME")
if [ -z "${NREAD}" -o $NREAD -lt 0 ]
then 
  echo "Q: returned ${NREAD}."; 
  exit 1;
elif [ $NREAD -eq 0 ]
then
  echo "Q: returned ${NREAD}. Nothing to do"; 
  exit 0;
fi
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "$DATASETID"

# run the transaction
SRCDATASET="iog-data-analytics.db_sync"
TARGETTBL="iog-data-analytics.cardano_mainnet.${TNAME}"
Q="
   BEGIN TRANSACTION;
   -- 1 insert new epoch
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${SRCDATASET}.${TMPTBL};
   -- 2 update the last index table
   UPDATE db_sync.last_index set last_epoch_no=${EPOCH_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

#DRYRUN="--dry_run"
DRYRUN=

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_${TNAME}-query.err > logs/update_${TNAME}-query.out

echo
echo "values inserted successfully for epoch: ${EPOCH_NO}"
echo "all done."
