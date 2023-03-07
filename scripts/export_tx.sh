#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
  SELECT epoch_no, 
         tx_hash,
         block_time, 
         slot_no, 
         txidx,
         out_sum, 
         fee, 
         deposit, 
         size,
         invalid_before, 
         invalid_after,
         valid_script, 
         script_size,
         count_inputs,
         count_outputs
  FROM analytics.vw_bq_tx
  WHERE block.epoch_no = ${EPOCH} "
}

process_epoch_f Q "tx" "iog-data-analytics.db_sync" 0
