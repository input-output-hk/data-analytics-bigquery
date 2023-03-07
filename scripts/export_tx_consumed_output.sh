#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
  SELECT b0.slot_no AS slot_no,
         tx0.block_index AS txidx,
         tx_in.tx_out_index AS index,
         b1.slot_no AS consumed_in_slot_no,
         tx1.block_index AS consumed_in_txidx
  FROM public.tx_in
  JOIN public.tx AS tx0 ON tx_in.tx_out_id = tx0.id
  JOIN block AS b0 on tx0.block_id = b0.id 
  JOIN public.tx AS tx1 ON tx_in.tx_in_id = tx1.id
  JOIN block AS b1 on tx1.block_id = b1.id 
  WHERE b1.epoch_no = ${EPOCH}"
}

process_epoch_f Q "tx_consumed_output" "iog-data-analytics.db_sync" 0
