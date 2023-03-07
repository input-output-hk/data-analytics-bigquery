#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      echo "SELECT epoch_no, slot_no, stake_address, stake_addr_hash
            FROM analytics.vw_bq_rel_stake_hash sa
            WHERE epoch_no = ${EPOCH} "
}

# the first delegation was in epoch 208
process_epoch_f Q "rel_stake_hash" "iog-data-analytics.db_sync" 242
