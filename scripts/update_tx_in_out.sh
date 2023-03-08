#!/bin/bash

if [ $# -lt 2 ]; then
	echo "$0 bq_block_height pg_block_height"
	exit 1
fi

set -e

source config.pg
source config.bq
source functions.sh


# BQ block height (TBD)
ACT_SLOT_NO=$1

# DB_SYNC block height (TBD)
CURR_SLOT_NO=$2

DELTA=$((CURR_SLOT_NO - ACT_SLOT_NO))
if [ $DELTA -lt 0 ]; then
	echo "something weird: bq@${ACT_SLOT_NO} pg@${CURR_SLOT_NO}!"
	exit 1
fi
if [ $DELTA -gt $CAP_SLOTS ]; then
	echo "too many slots in update, cap at $CAP_SLOTS"
	CURR_SLOT_NO=$((ACT_SLOT_NO + CAP_SLOTS))
fi

CLEAN_SLOT_NO=$((ACT_SLOT_NO - DELETE_SLOTS))
MAX_SLOT_NO=$((CURR_SLOT_NO - GRACE_SLOTS))

echo "BQ @ ${ACT_SLOT_NO}"
echo "   loading up to $MAX_SLOT_NO"
echo "   deleting from $CLEAN_SLOT_NO"

# in the following we prepare the data in temporary tables in dataset "db_sync"
# (not visible in the public schema)

PROJECTID="iog-data-analytics"
SCHEMA="tx_in_out"
SRCDATASET="db_sync"
TARGETDATASET="cardano_mainnet"
TARGETTBL="${PROJECTID}.${TARGETDATASET}.${SCHEMA}"

## 1 delete slots (no preparation needed)

## 2 insert slots (clean until max) into table: t3
TMPTBL="update_${SCHEMA}"
CSVNAME="update_${SCHEMA}"

Q="SELECT epoch_no, slot_no, txidx, inputs, outputs
   FROM analytics.vw_bq_tx_in_out
   WHERE slot_no >= ${CLEAN_SLOT_NO}
   AND slot_no <= ${MAX_SLOT_NO}"
NREAD=$(pg_query_to_csv "${Q}" "$CSVNAME")
if [ -z "${NREAD}" -o $NREAD -le 0 ]; then echo "Q: returned ${NREAD}."; (exit 1); fi
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "${PROJECTID}:${SRCDATASET}"

# run the transaction
Q="
   BEGIN TRANSACTION;
   -- 1 delete slots
   DELETE FROM ${TARGETTBL} WHERE slot_no >= ${CLEAN_SLOT_NO};
   -- 2 insert new slots
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${PROJECTID}.${SRCDATASET}.${TMPTBL};
   -- 3 update the last index table
   UPDATE db_sync.last_index set last_slot_no=${MAX_SLOT_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

#DRYRUN="--dry_run"
DRYRUN=

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id="${PROJECTID}:${TARGETDATASET}" --nouse_legacy_sql "${Q}" 2> logs/${CSVNAME}-query.err > logs/${CSVNAME}-query.out

echo
echo "the new block height: ${MAX_SLOT_NO}"
echo "all done."
