#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
  SELECT epoch_no, slot_no, block_hash
  FROM analytics.vw_bq_block_hash
  WHERE epoch_no = ${EPOCH}"
}

process_epoch_f Q "block_hash" "iog-data-analytics.db_sync" 330

