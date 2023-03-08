#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
  SELECT epoch_no, slot_no, txidx, tx_hash
  FROM analytics.vw_bq_tx_hash
  WHERE epoch_no = ${EPOCH} "
}

process_epoch_f Q "tx_hash" "iog-data-analytics.db_sync" 0
