#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
  SELECT epoch_no, slot_no, block_time, block_size, tx_count, sum_tx_fee, script_count, sum_script_size, pool_hash
  FROM analytics.vw_bq_block
  WHERE epoch_no = ${EPOCH} "
}

process_epoch_f Q "block" "iog-data-analytics.db_sync" 0
