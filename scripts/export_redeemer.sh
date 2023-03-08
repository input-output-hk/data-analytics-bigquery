#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
    SELECT epoch_no, slot_no, txidx, count, redeemers
    FROM analytics.vw_bq_redeemer 
    WHERE epoch_no = ${EPOCH} "
}

# starting from epoch 290
process_epoch_f Q "redeemer" "iog-data-analytics.db_sync" 290
