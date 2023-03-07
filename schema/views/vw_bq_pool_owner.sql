-- View: analytics.vw_bq_pool_owner

-- DROP VIEW analytics.vw_bq_pool_owner;

CREATE OR REPLACE VIEW analytics.vw_bq_pool_owner
AS
SELECT encode(ph.hash_raw, 'hex') as pool_hash,
       block.epoch_no as epoch_no,
       encode(sa.hash_raw, 'hex') as addr_hash,
       block.slot_no as slot_no,
       tx.block_index AS txidx
FROM public.pool_owner po
         JOIN public.pool_update pu ON po.pool_update_id = pu.id
         JOIN public.pool_hash ph ON pu.hash_id = ph.id
         JOIN public.stake_address sa on po.addr_id = sa.id
         JOIN public.tx on pu.registered_tx_id = tx.id
         JOIN public.block on tx.block_id = block.id
ORDER BY block.epoch_no, block.slot_no, tx.block_index, pool_hash, encode(sa.hash_raw, 'hex') ASC;

ALTER TABLE analytics.vw_bq_pool_owner
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_pool_owner TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_pool_owner TO db_sync_master;
