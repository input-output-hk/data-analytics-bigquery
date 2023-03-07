#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      if [ $EPOCH -eq 99999 ]; then
      echo "
    SELECT epoch_no, slot_no, txidx,
           datum_hash, bytes, value
    FROM analytics.vw_bq_datum
    ORDER BY epoch_no,slot_no ASC"
      else
	echo "SELECT NULL LIMIT 0"
      fi
}
function transform_csv() {
	local FNAME=$1
    $SED -i -e ':a /",/ { bb; }; /,"[^"]\+$/ { N; s/\n//g; ba; }; :b' ${FNAME}
    return 0
}

# do the query only once
process_epoch_f Q "datum" "iog-data-analytics.db_sync" 99999
