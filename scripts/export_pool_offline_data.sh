#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      if [ $EPOCH -eq 99999 ]; then
        echo "
  SELECT 
         pool_hash,
         epoch_no,
         ticker_name,
         json,
         metadata_url,
         metadata_hash,
         metadata_registered_tx_hash
  FROM analytics.vw_bq_pool_offline_data 
        "
      else
	echo "SELECT NULL LIMIT 0"
      fi
}

# do the query only once
process_epoch_f Q "pool_offline_data" "iog-data-analytics.db_sync" 99999