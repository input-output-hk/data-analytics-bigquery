#!/bin/bash

if [ $# -lt 2 ]; then
	echo "$0 bq_block_height pg_block_height"
	exit 1
fi

set -e

source config.pg
source config.bq
source functions.sh

TNAME="ma_minting"

# BQ block height (TBD)
ACT_SLOT_NO=$1

# DB_SYNC block height (TBD)
CURR_SLOT_NO=$2

DELTA=$((CURR_SLOT_NO - ACT_SLOT_NO))
if [ $DELTA -lt 1 ]; then
	echo "something weird: bq@${ACT_SLOT_NO} pg@${CURR_SLOT_NO}!"
	exit 1
fi
if [ $DELTA -gt $CAP_SLOTS ]; then
	echo "too many slots in update, cap at $CAP_SLOTS"
	CURR_SLOT_NO=$((ACT_SLOT_NO + CAP_SLOTS))
fi

# not deleting, just copying new records
MAX_SLOT_NO=$((CURR_SLOT_NO - GRACE_SLOTS))

echo "BQ @ ${ACT_SLOT_NO}"
echo "   loading from $ACT_SLOT_NO up to $MAX_SLOT_NO"

CSVNAME="update_${TNAME}-q1"
if [ -e "${CSVNAME}.csv" ]; then rm -f "${CSVNAME}.csv"; fi
SCHEMA="${TNAME}"
DATASETID="iog-data-analytics:db_sync"

## 1 delete slots 

## 2 insert slots (clean until max) into table
TMPTBL="tmp_${TNAME}_1"
Q="
    SELECT ma.fingerprint, encode(ma.policy,'hex') AS policyid, encode(ma.name,'base64') AS \"name_bytes\",
          subq.epoch_no, subq.minting
    FROM (
        SELECT mint.ident, b.epoch_no,
              json_agg(('{\"quantity\":'||mint.quantity::text||',\"slot_no\":'||b.slot_no::text||',\"txidx\":'||tx.block_index::text||'}')::json ORDER BY b.slot_no, tx.block_index ASC) AS minting
        FROM public.ma_tx_mint mint
        JOIN public.tx ON tx.id = mint.tx_id
        JOIN public.block b ON b.id = tx.block_id
        WHERE b.slot_no > ${ACT_SLOT_NO}
          AND b.slot_no <= ${MAX_SLOT_NO}
        GROUP BY b.epoch_no, mint.ident
    ) AS subq
    JOIN public.multi_asset ma ON ma.id = subq.ident "

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
   -- 1 delete slots
   -- none to delete as we do not store the slot number in the table
   -- 2 insert new slots
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${SRCDATASET}.${TMPTBL};
   -- 3 update the last index table
   UPDATE db_sync.last_index set last_slot_no=${MAX_SLOT_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

#DRYRUN="--dry_run"
DRYRUN=

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_${TNAME}-query.err > logs/update_${TNAME}-query.out

echo
echo "table's ${TNAME} new block height: ${MAX_SLOT_NO}"
echo "all done."
