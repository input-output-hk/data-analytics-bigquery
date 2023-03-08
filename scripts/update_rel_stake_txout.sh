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

DELETE_SLOTS=0
CLEAN_SLOT_NO=$((ACT_SLOT_NO - DELETE_SLOTS))
MAX_SLOT_NO=$((CURR_SLOT_NO - GRACE_SLOTS))

echo "BQ @ ${ACT_SLOT_NO}"
echo "   loading up to $MAX_SLOT_NO"
echo "   from $CLEAN_SLOT_NO"

CSVNAME="update_rel_stake_txout-q1"
if [ -e "${CSVNAME}.csv" ]; then rm -f "${CSVNAME}.csv"; fi
SCHEMA="rel_stake_txout"
DATASETID="iog-data-analytics:db_sync"

## 1 insert slots (clean until max) into table: tmp_rel_stake_txout_1
TMPTBL="tmp_rel_stake_txout_1"
Q="with dat AS
         (SELECT block.epoch_no,
                 sa.view        AS address,
                 block.slot_no,
                 tx.block_index AS txidx,
                 tx_out.index   AS idx,
                 (row_number() OVER (PARTITION BY block.epoch_no, sa.view ORDER BY block.slot_no, tx.block_index, tx_out.index) - 1) / 1000::INTEGER AS cluster_no
          FROM public.tx_out
                   JOIN public.tx ON tx.id = tx_out.tx_id
                   JOIN public.block ON block.id = tx.block_id
                   JOIN public.stake_address AS sa ON sa.id = tx_out.stake_address_id
          WHERE block.slot_no > ${CLEAN_SLOT_NO} AND block.slot_no <= ${MAX_SLOT_NO}
          ORDER BY block.epoch_no, sa.view, block.slot_no, tx.block_index, tx_out.index ASC
         )
   SELECT epoch_no,
          address,
          json_agg(
                  json_build_object('idx', idx,
                                    'slot_no', slot_no,
                                    'txidx', txidx)
                  order by slot_no, txidx, idx ASC
              ) as outputs
   FROM dat
   GROUP BY epoch_no, address, cluster_no
   ORDER BY epoch_no, address ASC
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
bq_load_csv "$CSVNAME" "$TMPTBL" "$SCHEMA" "$DATASETID"

# run the transaction
SRCDATASET="iog-data-analytics.db_sync"
TARGETTBL="iog-data-analytics.cardano_mainnet.rel_stake_txout"
Q="
   BEGIN TRANSACTION;
   -- 1 insert new slots
   INSERT INTO ${TARGETTBL}
   SELECT * FROM ${SRCDATASET}.${TMPTBL};
   -- 2 update the last index table
   UPDATE db_sync.last_index set last_slot_no=${MAX_SLOT_NO} WHERE tablename='${TARGETTBL}';
   COMMIT TRANSACTION;
"

#DRYRUN="--dry_run"
DRYRUN=

${BQ} query --bigqueryrc=$(pwd)/dot.bigqueryrc ${DRYRUN} --dataset_id=${DATASETID} --nouse_legacy_sql "${Q}" 2> logs/update_rel_stake_txout-query.err > logs/update_rel_stake_txout-query.out

echo
echo "the new block height: ${MAX_SLOT_NO}"
echo "all done."

function transform_csv() {
	local FNAME=$1
	$SED -i -e 's/,"{"/,"["/; s/"}"$/"]"/' ${FNAME}
	python3 split_long_lists.py -f ${FNAME} -i 2 >>/dev/stderr
	if [ -e split_${FNAME} ]; then
		mv split_${FNAME} ${FNAME}
	else
		echo "failed to split CSV file: ${FNAME}"
		exit 1
	fi
   return 0
}
