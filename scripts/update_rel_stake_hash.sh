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

# not deleting, just copying new records
CLEAN_SLOT_NO=$((ACT_SLOT_NO - DELETE_SLOTS))
MAX_SLOT_NO=$((CURR_SLOT_NO - GRACE_SLOTS))

echo "BQ @ ${ACT_SLOT_NO}"
echo "   loading up to $MAX_SLOT_NO"
echo "   deleting from $CLEAN_SLOT_NO"

# in the following we prepare the data in temporary tables in dataset "db_sync"
# (not visible in the public schema)

PROJECTID="iog-data-analytics"
SCHEMA="rel_stake_hash"
SRCDATASET="db_sync"
TARGETDATASET="cardano_mainnet"
TARGETTBL="${PROJECTID}.${TARGETDATASET}.${SCHEMA}"

## 1 delete slots 

## 2 insert slots (clean until max) into temporary table

function transform_csv() {
	local FNAME=$1
    $SED -i -e ':a /",/ { bb; }; /,"[^"]\+$/ { N; s/\n//g; ba; }; :b' ${FNAME}
    return 0
}

TMPTBL="update_${SCHEMA}"
CSVNAME="update_${SCHEMA}"

Q="SELECT epoch_no, slot_no, stake_address, stake_addr_hash
   FROM analytics.vw_bq_rel_stake_hash
   WHERE slot_no >= ${CLEAN_SLOT_NO}
     AND slot_no <= ${MAX_SLOT_NO}"
NREAD=$(pg_query_to_csv "${Q}" "$CSVNAME")
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
  ${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id="${PROJECTID}:${SRCDATASET}" --nouse_legacy_sql "${Q}" 2> logs/${CSVNAME}-query.err > logs/${CSVNAME}-query.out
  echo "table's ${TNAME} new block height: ${MAX_SLOT_NO}"
  exit 0;
fi
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "${PROJECTID}:${SRCDATASET}"


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
echo "table's ${SCHEMA} new block height: ${MAX_SLOT_NO}"
echo "all done."
