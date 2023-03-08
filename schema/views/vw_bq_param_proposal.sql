-- View: analytics.vw_bq_param_proposal

-- DROP VIEW analytics.vw_bq_param_proposal;

CREATE OR REPLACE VIEW analytics.vw_bq_param_proposal
AS
SELECT pp.epoch_no,
       encode(key,'hex') AS key,
       min_fee_a, min_fee_b,
       max_block_size,
       max_tx_size,
       max_bh_size,
       key_deposit, pool_deposit,
       max_epoch,
       optimal_pool_count,
       influence,
       monetary_expand_rate,
       treasury_growth_rate,
       decentralisation,
       encode(entropy,'hex') AS entropy,
       protocol_major, protocol_minor,
       min_utxo_value,
       min_pool_cost,
       coins_per_utxo_size,
       cm.costs AS cost_model,
       price_mem,
       price_step,
       max_tx_ex_mem, max_tx_ex_steps,
       max_block_ex_mem, max_block_ex_steps,
       max_val_size,
       collateral_percent,
       max_collateral_inputs,
       block.slot_no AS registered_tx_slot_no,
       tx.block_index AS registered_tx_index
FROM public.param_proposal pp
         JOIN public.tx tx ON tx.id = registered_tx_id
         JOIN public.block block ON block.id = tx.block_id
         LEFT JOIN public.cost_model cm ON cm.id = cost_model_id
ORDER BY pp.epoch_no, block.slot_no, tx.block_index, pp.key ASC;

ALTER TABLE analytics.vw_bq_param_proposal
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_param_proposal TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_param_proposal TO db_sync_master;
