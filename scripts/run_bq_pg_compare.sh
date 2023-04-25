#!/bin/bash

set -e

export PGPASSWORD=$(jq -r .password <<< "$DB_CONFIG")
export PGHOST=$(jq -r .host <<< "$DB_CONFIG")
export PGDATABASE=$(jq -r .dbname <<< "$DB_CONFIG")
export PGDATABASE_TESTNET=$(jq -r .dbname_test <<< "$DB_CONFIG")
export PGPORT=$(jq -r .port <<< "$DB_CONFIG")
export PGUSER=$(jq -r .username <<< "$DB_CONFIG")

source config.pg

SNS_ACCESS_KEY=`echo ${SNS_CONFIG} | jq '.aws_access_key_id' | sed -e 's/^"//' -e 's/"$//'`
export SNS_ACCESS_KEY
SNS_SECRET_KEY=`echo ${SNS_CONFIG} | jq '.aws_secret_access_key' | sed -e 's/^"//' -e 's/"$//'`
export SNS_SECRET_KEY
SNS_TOPIC=`echo ${SNS_CONFIG} | jq '.topic' | sed -e 's/^"//' -e 's/"$//'`
export SNS_TOPIC
SNS_TOPIC_ARN=`echo ${SNS_CONFIG} | jq '.topic_arn' | sed -e 's/^"//' -e 's/"$//'`
export SNS_TOPIC_ARN

AWS_ACCESS_KEY_ID=`echo ${AWS_CONFIG} | jq '.aws_access_key_id' | sed -e 's/^"//' -e 's/"$//'`
export AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=`echo ${AWS_CONFIG} | jq '.aws_secret_access_key' | sed -e 's/^"//' -e 's/"$//'`
export AWS_SECRET_ACCESS_KEY
AWS_BUCKET_NAME=$(jq -r .aws_s3_bucket_bq_pg_compare <<< "$AWS_CONFIG")
export AWS_BUCKET_NAME

export BQUSER=$(jq -r .client_email <<< "$BQ_CONFIG")
echo $BQ_CONFIG > ./key.json
gcloud auth activate-service-account $BQUSER --key-file ./key.json 
${BQ} ls

declare -a TablesPerEpoch=(
		"delegation"
		"epoch_param"
		"ada_pots"
		"ma_minting"
		"param_proposal"
		"pool_offline_data"
		"pool_update"
                "rel_addr_txout"
		"rel_stake_txout"
		"reward"
	   )
declare -a TablesPerSlot=(
		"block"
		"block_hash"
		"collateral"
		"datum"
		"pool_owner"
		"pool_retire"
		"redeemer"
		"rel_stake_hash"
		"script"
		"stake_registration"
		"stake_deregistration"
		"tx"
		"tx_hash"
		"tx_in_out"
		"tx_metadata"
		"withdrawal"
	   )
declare -a TablesPerSlotOnly=(
		"tx_consumed_output"
	   )

res2=$(${PSQL} -c "SELECT max(epoch_no) as max_epoch, max(slot_no) as max_slot from public.block;")
PG_EPOCH=$(echo ${res2} | ${SED} -ne 's/^max_epoch | max_slot --*+--* \([0-9][0-9]*\).*/\1/p;')
PG_SLOT=$(echo ${res2} | ${SED} -ne 's/^max_epoch | max_slot --*+--* \([0-9][0-9]*\) | \([0-9][0-9]*\).*/\2/p;')

echo "Running Postgres-BigQuery comparison"
echo "BigQuery - Postgres comparison:" > msg.txt
echo "Postgres @ ${PG_EPOCH} epoch, ${PG_SLOT}" >> msg.txt
echo "postgres_epoch ${PG_EPOCH}" >> metrics.txt
echo "postgres_slot ${PG_SLOT}" >> metrics.txt

## use for loop to read all tables
for (( i=0; i<${#TablesPerSlot[@]}; i++ ));
do
  TABLE=${TablesPerSlot[$i]}
  TABLNAME="iog-data-analytics.cardano_mainnet.${TABLE}"
  res=$(${BQ} --format=json query --nouse_legacy_sql "SELECT max(slot_no) as max_slot_no, max(epoch_no) as max_epoch_no FROM ${TABLNAME}")
  BQ_SLOT=$(echo ${res} | jq -r '.[0].max_slot_no')
  BQ_EPOCH=$(echo ${res} | jq -r '.[0].max_epoch_no')
  echo "$TABLNAME @ $BQ_EPOCH epoch, $BQ_SLOT slot" >> msg.txt
  echo "${TABLE}_epoch $BQ_EPOCH" >> metrics.txt
  echo "${TABLE}_slot $BQ_SLOT" >> metrics.txt
done

for (( i=0; i<${#TablesPerEpoch[@]}; i++ ));
do
  TABLE=${TablesPerEpoch[$i]}
  TABLNAME="iog-data-analytics.cardano_mainnet.${TABLE}"
  res=$(${BQ} --format=json query --nouse_legacy_sql "SELECT max(epoch_no) as max_epoch_no FROM ${TABLNAME}")
  BQ_EPOCH=$(echo ${res} | jq -r '.[0].max_epoch_no')
  echo "$TABLNAME @ $BQ_EPOCH epoch" >> msg.txt
  echo "${TABLE}_epoch $BQ_EPOCH" >> metrics.txt
done

for (( i=0; i<${#TablesPerSlotOnly[@]}; i++ ));
do
  TABLE=${TablesPerSlotOnly[$i]}
  TABLNAME="iog-data-analytics.cardano_mainnet.${TABLE}"
  res=$(${BQ} --format=json query --nouse_legacy_sql "SELECT max(slot_no) as max_slot_no FROM ${TABLNAME}")
  BQ_SLOT=$(echo ${res} | jq -r '.[0].max_slot_no')
  echo "$TABLNAME @ $BQ_SLOT slot" >> msg.txt
  echo "${TABLE}_slot $BQ_SLOT" >> metrics.txt
done

aws sns publish --topic-arn $SNS_TOPIC_ARN --message file://msg.txt
aws s3 cp ./metrics.txt "$AWS_BUCKET_NAME/metrics.txt"
echo "all done."
