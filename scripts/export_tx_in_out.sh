#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
   SELECT epoch_no, slot_no, txidx, inputs, outputs
   FROM analytics.vw_bq_tx_in_out
   WHERE epoch_no = ${EPOCH} "
}

process_epoch_f Q "tx_in_out" "iog-data-analytics.db_sync" 287
