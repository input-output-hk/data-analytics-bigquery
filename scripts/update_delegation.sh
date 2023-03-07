#!/bin/bash

if [ $# -lt 2 ]; then
	echo "$0 bq_block_height pg_block_height"
	exit 1
fi

set -e

source config.pg
source config.bq
source functions.sh

TNAME="delegation"

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
  SELECT block.epoch_no as epoch_no,
   encode(stake_address.hash_raw, 'hex') as stake_addr_hash,
  json_agg(
    json_build_object(
      'pool_hash', encode(pool_hash.hash_raw, 'hex'),
      'cert_index', delegation.cert_index,
      'active_epoch_no', delegation.active_epoch_no,
      'slot_no', delegation.slot_no,
      'txidx', tx.block_index
  ) ORDER BY delegation.active_epoch_no, delegation.slot_no, tx.block_index ASC) AS delegations
  FROM public.delegation
  JOIN public.stake_address ON delegation.addr_id = stake_address.id
  JOIN pool_hash ON delegation.pool_hash_id = pool_hash.id 
  JOIN tx ON delegation.tx_id = tx.id 
  JOIN block ON tx.block_id = block.id
  WHERE block.slot_no > ${CLEAN_SLOT_NO}
   AND block.slot_no <= ${MAX_SLOT_NO}
    GROUP BY block.epoch_no, encode(stake_address.hash_raw, 'hex')"
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
