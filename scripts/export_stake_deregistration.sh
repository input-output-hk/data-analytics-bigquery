#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "
SELECT epoch_no, stake_addr_hash, cert_index, slot_no, txidx  
FROM analytics.vw_bq_stake_deregistration
WHERE epoch_no = ${EPOCH}
        "
}

# stake deregistration started in epoch 209
process_epoch_f Q "stake_deregistration" "iog-data-analytics.db_sync" 209
