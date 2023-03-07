#!/bin/bash

if [ $# -lt 2 ]; then
	echo "$0 bq_block_height pg_block_height"
	exit 1
fi

set -e

source config.pg
source config.bq
source functions.sh

TNAME="pool_offline_data"

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

# not deleting, just copying new records
DELETE_SLOTS=0
CLEAN_SLOT_NO=$((ACT_SLOT_NO - DELETE_SLOTS))
MAX_SLOT_NO=$((CURR_SLOT_NO - GRACE_SLOTS))

echo "BQ @ ${ACT_SLOT_NO}"
echo "   loading up to $MAX_SLOT_NO"
echo "   deleting from $CLEAN_SLOT_NO"

CSVNAME="update_${TNAME}-q1"
if [ -e "${CSVNAME}.csv" ]; then rm -f "${CSVNAME}.csv"; fi
SCHEMA="${TNAME}"
DATASETID="iog-data-analytics:db_sync"

## 1 delete slots

## 2 insert slots (clean until max) into temporary table
TMPTBL="tmp_${TNAME}_1"
Q="
  SELECT
         encode(ph.hash_raw, 'hex') AS pool_hash,
         block.epoch_no AS epoch_no,
         pod.ticker_name AS ticker_name,
         pod.json AS json,
         pmr.url AS metadata_url,
         encode(pmr.hash, 'base64') AS metadata_hash,
         encode(tx.hash,'hex') AS metadata_registered_tx_hash
  FROM public.pool_offline_data AS pod
  LEFT JOIN public.pool_metadata_ref pmr ON pod.pmr_id = pmr.id
  JOIN public.pool_hash ph ON pod.pool_id = ph.id
  JOIN tx ON pmr.registered_tx_id = tx.id
  JOIN block ON tx.block_id = block.id
  WHERE block.slot_no > ${CLEAN_SLOT_NO}
   AND block.slot_no <= ${MAX_SLOT_NO}"
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
   UPDATE db_sync.last_index set last_slot_no=${MAX_SLOT_NO} WHERE tablename='${TARGETTBL}';"
  ${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_${TNAME}-query.err > logs/update_${TNAME}-query.out
  echo "table's ${TNAME} new block height: ${MAX_SLOT_NO}"
  exit 0;
fi
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "$DATASETID"

# run the transaction
SRCDATASET="iog-data-analytics.db_sync"
Q="
   BEGIN TRANSACTION;
   -- 1 delete slots
   -- none to delete as we do not store the slot number in the table
   -- 2  insert new slots
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${SRCDATASET}.${TMPTBL};
   -- 3 update the last index table
   UPDATE db_sync.last_index set last_slot_no=${MAX_SLOT_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_${TNAME}-query.err > logs/update_${TNAME}-query.out

echo
echo "table's ${TNAME} new block height: ${MAX_SLOT_NO}"
echo "all done."
