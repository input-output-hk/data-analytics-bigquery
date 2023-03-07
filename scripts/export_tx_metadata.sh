#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
    SELECT block.epoch_no, encode(tx.hash,'hex') AS \"tx_hash\",
           block.slot_no, tx.block_index AS txidx, subq.metadata
    FROM (
        SELECT tx_id,
            json_agg(('{\"index\":'||key::text||',\"meta\":'||json::text||'}')::json) AS metadata
        FROM public.tx_metadata
        JOIN public.tx itx ON itx.id = tx_id
        JOIN public.block ib ON ib.id = itx.block_id
        WHERE ib.epoch_no = ${EPOCH}
        GROUP BY tx_id
        ORDER BY tx_id ASC
    ) AS subq
    JOIN public.tx ON tx.id = subq.tx_id
    JOIN public.block ON block.id = tx.block_id AND block.epoch_no = ${EPOCH} "
}

# starting from 211, continuing from 216
process_epoch_f Q "tx_metadata" "iog-data-analytics.db_sync" 216
