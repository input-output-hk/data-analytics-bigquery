-- View: analytics.vw_bq_epoch_param

-- DROP VIEW analytics.vw_bq_epoch_param;

CREATE OR REPLACE VIEW analytics.vw_bq_epoch_param
AS
SELECT epoch_no, min_fee_a, min_fee_b,
       max_block_size, max_tx_size, max_bh_size,
       key_deposit, pool_deposit, max_epoch,
       optimal_pool_count, influence,
       monetary_expand_rate, treasury_growth_rate,
       decentralisation,
       encode(extra_entropy,'hex') AS extra_entropy,
       protocol_major, protocol_minor,
       min_utxo_value, min_pool_cost,
       encode(nonce,'hex') AS nonce,
       coins_per_utxo_size, cm.costs as cost_model,
       price_mem, price_step,
       max_tx_ex_mem, max_tx_ex_steps,
       max_block_ex_mem, max_block_ex_steps,
       max_val_size,
       collateral_percent, max_collateral_inputs
FROM public.epoch_param ep
LEFT JOIN public.cost_model cm ON cm.id = cost_model_id
ORDER BY epoch_no ASC;

ALTER TABLE analytics.vw_bq_epoch_param
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_epoch_param TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_epoch_param TO db_sync_master;
