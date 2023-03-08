#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
      SELECT epoch_no, 
             slot_no, 
             txidx,
             script_hash, 
             type,
             json,
             bytes,
             serialised_size
      FROM analytics.vw_bq_script
      WHERE epoch_no = ${EPOCH} "
}

function transform_csv() {
	local FNAME=$1
    $SED -i -e ':a /",/ { bb; }; /,"[^"]\+$/ { N; s/\n//g; ba; }; :b' ${FNAME}
    return 0
}

# scripts started in epoch 238(1), but really from epoch 251
process_epoch_f Q "script" "iog-data-analytics.db_sync" 238
