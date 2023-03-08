-- View: analytics.vw_bq_pool_update

-- DROP VIEW analytics.vw_bq_pool_update;

CREATE OR REPLACE VIEW analytics.vw_bq_pool_update
AS
SELECT pu.active_epoch_no as active_epoch_no,
       encode(ph.hash_raw, 'hex') as pool_hash,
       pu.cert_index as cert_index,
       encode(pu.vrf_key_hash, 'hex') as vrf_key_hash,
       pu.pledge::decimal(20, 0) as pledge,
       encode(sa.hash_raw, 'hex') as reward_addr,
       pu.margin::decimal(30, 9) as margin,
       pu.fixed_cost::decimal(20, 0) as fixed_cost,
       encode(tx.hash, 'hex') as registered_tx_hash,
       block.epoch_no as epoch_no,
       pmr.url as metadata_url,
       encode(pmr.hash, 'hex') as metadata_hash,
       encode(tx_meta.hash,'hex') AS metadata_registered_tx_hash
FROM public.pool_update pu
         JOIN public.stake_address sa ON sa.id = pu.reward_addr_id
         JOIN public.pool_hash ph ON pu.hash_id = ph.id
         JOIN public.pool_metadata_ref pmr ON pu.meta_id = pmr.id
         JOIN public.tx ON pu.registered_tx_id = tx.id
         JOIN public.block ON tx.block_id = block.id
         JOIN public.tx tx_meta ON pmr.registered_tx_id = tx_meta.id
ORDER BY block.epoch_no, pool_hash, registered_tx_hash, cert_index ASC;

ALTER TABLE analytics.vw_bq_pool_update
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_pool_update TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_pool_update TO db_sync_master;
