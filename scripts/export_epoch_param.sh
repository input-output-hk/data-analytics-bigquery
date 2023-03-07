#!/bin/bash

set -e

source config.pg
source functions.sh

function Q() {
      local EPOCH=$1
      if [ $EPOCH -eq 99999 ]; then
        echo "
  SELECT epoch_no, min_fee_a, min_fee_b,
         max_block_size, max_tx_size, max_bh_size,
         key_deposit, pool_deposit, max_epoch,
         optimal_pool_count, influence, 
         monetary_expand_rate, treasury_growth_rate,
         decentralisation,
         extra_entropy,
         protocol_major, protocol_minor,
         min_utxo_value, min_pool_cost,
         nonce,
         coins_per_utxo_size, cost_model,
         price_mem, price_step,
         max_tx_ex_mem, max_tx_ex_steps,
         max_block_ex_mem, max_block_ex_steps,
         max_val_size,
         collateral_percent, max_collateral_inputs
  FROM analytics.vw_bq_epoch_param
        "
      else
	echo "SELECT NULL LIMIT 0"
      fi
}

# do the query only once
process_epoch_f Q "epoch_param" "iog-data-analytics.db_sync" 99999
