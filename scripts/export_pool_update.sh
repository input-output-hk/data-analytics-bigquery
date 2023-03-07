#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      if [ $EPOCH -eq 99999 ]; then
        echo "
 SELECT active_epoch_no,
        pool_hash,
        cert_index,
        vrf_key_hash,
        pledge,
        reward_addr,
        margin,
        fixed_cost,
        registered_tx_hash,
        epoch_no,
        metadata_url,
        metadata_hash,
        metadata_registered_tx_hash
  FROM analytics.vw_bq_pool_update
        "
      else
	echo "SELECT NULL LIMIT 0"
      fi
}

# do the query only once
process_epoch_f Q "pool_update" "iog-data-analytics.db_sync" 99999
