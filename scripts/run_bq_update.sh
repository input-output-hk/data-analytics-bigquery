#!/bin/bash

set -e

export PGPASSWORD=$(jq -r .password <<< "$DB_CONFIG")
export PGHOST=$(jq -r .host <<< "$DB_CONFIG")
export PGDATABASE=$(jq -r .dbname <<< "$DB_CONFIG")
export PGDATABASE_TESTNET=$(jq -r .dbname_test <<< "$DB_CONFIG")
export PGPORT=$(jq -r .port <<< "$DB_CONFIG")
export PGUSER=$(jq -r .username <<< "$DB_CONFIG")

source config.pg
source config.bq

export BQUSER=$(jq -r .client_email <<< "$BQ_CONFIG")
echo $BQ_CONFIG > /usr/src/app/scripts/key.json
gcloud auth activate-service-account $BQUSER --key-file /usr/src/app/scripts/key.json 
${BQ} ls

res2=$(${PSQL} -c "SELECT max(slot_no) as max_slot, max(epoch_no) as max_epoch from public.block;")
ENDING_SLOT=$(echo ${res2} | ${SED} -ne 's/^max_slot | max_epoch --*+--* \([0-9][0-9]*\).*/\1/p;')
PG_EPOCH=$(echo ${res2} | ${SED} -ne 's/^max_slot | max_epoch --*+--* \([0-9][0-9]*\) | \([0-9][0-9]*\).*/\2/p;')
ENDING_SLOT_MINUS_GRACE=$((ENDING_SLOT - GRACE_SLOTS))

declare -a Tables=("tx" "tx_in_out" "tx_consumed_output" "tx_hash" "tx_metadata" "block" "block_hash" "rel_addr_txout" "rel_stake_txout" "rel_stake_hash" "collateral" "ma_minting" "script" "pool_offline_data" "pool_owner" "pool_retire" "pool_update" "redeemer" "stake_registration" "stake_deregistration" "withdrawal" "delegation" "datum")
# use for loop to read all tables
for (( i=0; i<${#Tables[@]}; i++ ));
do
  TABLE=${Tables[$i]}
  TABLNAME="iog-data-analytics.cardano_mainnet.${TABLE}"
  res=$(${BQ} --format=json query --nouse_legacy_sql "SELECT last_slot_no FROM iog-data-analytics.db_sync.last_index where tablename = '${TABLNAME}'")
  STARTING_SLOT=$(echo ${res} | jq -r '.[0].last_slot_no')
  COUNT=0
  while [ "$COUNT" -lt 3 ] && [ "$STARTING_SLOT" -lt "$ENDING_SLOT_MINUS_GRACE" ]
  do
    STARTING_SLOT=$(echo ${res} | jq -r '.[0].last_slot_no')
    SCRIPT="./update_${TABLE}.sh"
    echo "Updating ${TABLNAME} since ${STARTING_SLOT} slot until ${ENDING_SLOT}"
    ${SCRIPT} ${STARTING_SLOT} ${ENDING_SLOT}
    res=$(${BQ} --format=json query --nouse_legacy_sql "SELECT last_slot_no FROM iog-data-analytics.db_sync.last_index where tablename = '${TABLNAME}'")
    STARTING_SLOT=$(echo ${res} | jq -r '.[0].last_slot_no')
    COUNT=$((COUNT + 1))
  done
done
echo "Updating db-sync slot_no to ${ENDING_SLOT} and epoch_no to ${PG_EPOCH} in BigQuery"
Q="UPDATE iog-data-analytics.db_sync.last_index set last_slot_no=${ENDING_SLOT}, last_epoch_no=${PG_EPOCH} WHERE tablename='db-sync';"
${BQ} query --nouse_legacy_sql "${Q}"
echo "All done."
