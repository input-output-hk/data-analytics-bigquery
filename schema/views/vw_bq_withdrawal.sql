-- View: analytics.vw_bq_withdrawal

-- DROP VIEW analytics.vw_bq_withdrawal;

CREATE OR REPLACE VIEW analytics.vw_bq_withdrawal
AS
SELECT block.epoch_no AS epoch_no,
       encode(stake_address.hash_raw, 'hex') as stake_addr_hash,
       w.amount::bigint AS amount,
       block.slot_no AS slot_no,
       tx.block_index AS txidx
FROM public.withdrawal w
         JOIN public.stake_address ON w.addr_id = stake_address.id
         JOIN public.tx ON w.tx_id = tx.id
         JOIN public.block ON tx.block_id = block.id
ORDER BY epoch_no, slot_no, txidx, stake_addr_hash ASC;

ALTER TABLE analytics.vw_bq_withdrawal
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_withdrawal TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_withdrawal TO db_sync_master;
