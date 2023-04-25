#!/bin/bash

set -e

export PGPASSWORD=$(jq -r .password <<< "$DB_CONFIG")
export PGHOST=$(jq -r .host <<< "$DB_CONFIG")
export PGDATABASE=$(jq -r .dbname <<< "$DB_CONFIG")
export PGDATABASE_TESTNET=$(jq -r .dbname_test <<< "$DB_CONFIG")
export PGPORT=$(jq -r .port <<< "$DB_CONFIG")
export PGUSER=$(jq -r .username <<< "$DB_CONFIG")

source config.pg

export BQUSER=$(jq -r .client_email <<< "$BQ_CONFIG")
echo $BQ_CONFIG > /usr/src/app/scripts/key.json
gcloud auth activate-service-account $BQUSER --key-file /usr/src/app/scripts/key.json 
${BQ} ls

declare -a Tables=("epoch_param" "ada_pots" "param_proposal" "ma_minting" "pool_update" "pool_offline_data" "delegation" "reward" "rel_addr_txout" "rel_stake_txout" )
 
# use for loop to read all tables
for (( i=0; i<${#Tables[@]}; i++ ));
do
  TABLE=${Tables[$i]}
  TABLNAME="iog-data-analytics.cardano_mainnet.${TABLE}"
  res=$(${BQ} --format=json query --nouse_legacy_sql "SELECT last_epoch_no FROM iog-data-analytics.db_sync.last_index where tablename = '${TABLNAME}'")
  BQ_EPOCH_NO=$(echo ${res} | jq -r '.[0].last_epoch_no')
  
  res2=$(${PSQL} -c "SELECT max(epoch_no) as max_epoch_no from public.block;")
  PG_EPOCH_NO=$(echo ${res2} | ${SED} -ne 's/^max_epoch_no --* \([0-9][0-9]*\).*/\1/p;')
  
  if [ "${PG_EPOCH_NO}" -gt "${BQ_EPOCH_NO}" ]
  then
    SCRIPT="./update_epoch_${TABLE}.sh"
    echo "Updating ${TABLNAME}. BigQuery epoch: ${BQ_EPOCH_NO} - Postgres epoch: ${PG_EPOCH_NO}"
    ${SCRIPT} ${BQ_EPOCH_NO} ${PG_EPOCH_NO}
  fi
done
