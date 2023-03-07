#!/bin/bash

set -e

DATE=$(date +"%s")
CHECK=$((DATE / 86400 % 5 ))
if [ $CHECK -ne 3 ]; then
    echo "Not an epoch boundary. Exiting..."
    exit 0
else
    echo "Epoch boundary, running BQ/PG deep comparison"
fi

export PGPASSWORD=$(jq -r .password <<< "$DB_CONFIG")
export PGHOST=$(jq -r .host <<< "$DB_CONFIG")
export PGDATABASE=$(jq -r .dbname <<< "$DB_CONFIG")
export PGDATABASE_TESTNET=$(jq -r .dbname_test <<< "$DB_CONFIG")
export PGPORT=$(jq -r .port <<< "$DB_CONFIG")
export PGUSER=$(jq -r .username <<< "$DB_CONFIG")

export BQUSER=$(jq -r .client_email <<< "$BQ_CONFIG")
export BQPROJECT=$(jq -r .project_id <<< "$BQ_CONFIG")
echo $BQ_CONFIG > /usr/src/app/scripts/key.json

SNS_ACCESS_KEY=`echo ${SNS_CONFIG} | jq '.aws_access_key_id' | sed -e 's/^"//' -e 's/"$//'`
export SNS_ACCESS_KEY
SNS_SECRET_KEY=`echo ${SNS_CONFIG} | jq '.aws_secret_access_key' | sed -e 's/^"//' -e 's/"$//'`
export SNS_SECRET_KEY
SNS_TOPIC=`echo ${SNS_CONFIG} | jq '.topic' | sed -e 's/^"//' -e 's/"$//'`
export SNS_TOPIC
SNS_TOPIC_ARN=`echo ${SNS_CONFIG} | jq '.topic_arn' | sed -e 's/^"//' -e 's/"$//'`
export SNS_TOPIC_ARN

EPOCH_NO=$1
python3 ./deep_compare/bq_pg_deep_compare.py $EPOCH_NO
aws sns publish --topic-arn $SNS_TOPIC_ARN --message file://msg.txt
