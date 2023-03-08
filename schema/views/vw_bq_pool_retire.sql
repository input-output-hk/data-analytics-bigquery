-- View: analytics.vw_bq_pool_retire

-- DROP VIEW analytics.vw_bq_pool_retire;

CREATE OR REPLACE VIEW analytics.vw_bq_pool_retire
AS
SELECT encode(ph.hash_raw, 'hex') as pool_hash,
       pr.retiring_epoch as retiring_epoch,
       block.epoch_no as epoch_no,
       pr.cert_index as cert_index,
       encode(tx.hash, 'hex') as announced_tx_hash,
       block.slot_no as slot_no,
       tx.block_index announced_txidx
FROM public.pool_retire pr
         JOIN public.pool_hash ph on pr.hash_id = ph.id
         JOIN public.tx on pr.announced_tx_id = tx.id
         JOIN public.block on tx.block_id = block.id
ORDER BY block.epoch_no, block.slot_no, tx.block_index, pool_hash ASC;

ALTER TABLE analytics.vw_bq_pool_retire
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_pool_retire TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_pool_retire TO db_sync_master;
