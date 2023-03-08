-- View: analytics.vw_bq_stake_deregistration

-- DROP VIEW analytics.vw_bq_stake_deregistration;

CREATE OR REPLACE VIEW analytics.vw_bq_stake_deregistration
AS
SELECT sd.epoch_no AS epoch_no,
       encode(sa.hash_raw, 'hex') AS stake_addr_hash,
       sd.cert_index AS cert_index,
       block.slot_no AS slot_no,
       tx.block_index AS txidx
FROM public.stake_deregistration sd
         JOIN public.stake_address sa ON sd.addr_id = sa.id
         JOIN public.tx ON sd.tx_id = tx.id
         JOIN public.block ON tx.block_id = block.id
ORDER BY block.epoch_no, block.slot_no, tx.block_index, encode(sa.hash_raw, 'hex'), sd.cert_index ASC;

ALTER TABLE analytics.vw_bq_stake_deregistration
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_stake_deregistration TO PUBLIC;
GRANT SELECT ON TABLE analytics.vw_bq_stake_deregistration TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_stake_deregistration TO db_sync_master;
