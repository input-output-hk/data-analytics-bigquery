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

# not going to delete entries based on slot_no
DELETE_SLOTS=0
CLEAN_SLOT_NO=$((ACT_SLOT_NO - DELETE_SLOTS))
MAX_SLOT_NO=$((CURR_SLOT_NO - GRACE_SLOTS))

echo "BQ @ ${ACT_SLOT_NO}"
echo "   loading up to $MAX_SLOT_NO"
echo "   from $CLEAN_SLOT_NO"

# in the following we prepare the data in temporary tables in dataset "db_sync"
# (not visible in the public schema)

PROJECTID="iog-data-analytics"
SCHEMA="rel_addr_txout"
SRCDATASET="db_sync"
TARGETDATASET="cardano_mainnet"
TARGETTBL="${PROJECTID}.${TARGETDATASET}.${SCHEMA}"

TMPTBL="update_${SCHEMA}"
CSVNAME="update_${SCHEMA}"

## 1 insert slots (clean until max) into temporary table

function transform_csv() {
	local FNAME=$1
    $SED -i -e ':a /",/ { bb; }; /,"[^"]\+$/ { N; s/\n//g; ba; }; :b' ${FNAME}
    return 0
}

Q="
  WITH dat AS
            (SELECT block.epoch_no AS _epoch_no,
                    tx_out.address AS _address,
                    tx_out.address_has_script AS _addr_scr,
                    COALESCE(encode(tx_out.payment_cred,'hex'),NULL) AS _paym_cred,
                    COALESCE(encode(tx_out.data_hash,'hex'),NULL) AS _data_hash,
                    COALESCE(encode(datum.hash,'hex'),NULL) AS _inline_datum,
                    COALESCE(encode(script.hash,'hex'),NULL) AS _inline_script,
                    block.slot_no,
                    tx.block_index AS txidx,
                    tx_out.index   AS idx,
                    (row_number() OVER (PARTITION BY block.epoch_no, tx_out.address
                                        ORDER BY block.slot_no, tx.block_index, tx_out.index) - 1) / 1000::INTEGER AS cluster_no
            FROM public.tx_out
            JOIN public.tx ON tx.id = tx_out.tx_id
            JOIN public.block ON block.id = tx.block_id
            LEFT JOIN public.datum ON datum.id = tx_out.inline_datum_id
            LEFT JOIN public.script ON script.id = tx_out.reference_script_id
            WHERE block.slot_no > ${CLEAN_SLOT_NO} AND block.slot_no <= ${MAX_SLOT_NO}
            ORDER BY block.epoch_no, tx_out.address, block.slot_no, tx.block_index, tx_out.index ASC
            )
  SELECT _epoch_no,
          _address,
          _addr_scr,
          _paym_cred,
          _data_hash,
          _inline_datum,
          _inline_script,
          json_agg(
                  json_build_object('idx', idx,
                                    'slot_no', slot_no,
                                    'txidx', txidx)
                  order by slot_no, txidx, idx ASC
              ) AS _outputs
  FROM dat
  GROUP BY _epoch_no, _address, _addr_scr, _paym_cred, _data_hash, _inline_datum, _inline_script, cluster_no
  ORDER BY _epoch_no, _address ASC
      "
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
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "${PROJECTID}:${SRCDATASET}"

# run the transaction
Q="
   BEGIN TRANSACTION;
   -- 1 insert new slots
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${PROJECTID}.${SRCDATASET}.${TMPTBL};
   -- 2 update the last index table
   UPDATE db_sync.last_index set last_slot_no=${MAX_SLOT_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

#DRYRUN="--dry_run"
DRYRUN=

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id="${PROJECTID}:${TARGETDATASET}" --nouse_legacy_sql "${Q}" 2> logs/${CSVNAME}-query.err > logs/${CSVNAME}-query.out

echo
echo "table's ${SCHEMA} new block height: ${MAX_SLOT_NO}"
echo "all done."
