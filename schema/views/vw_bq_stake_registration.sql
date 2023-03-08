-- View: analytics.vw_bq_stake_registration

-- DROP VIEW analytics.vw_bq_stake_registration;

CREATE OR REPLACE VIEW analytics.vw_bq_stake_registration
AS
SELECT sr.epoch_no AS epoch_no,
       encode(sa.hash_raw, 'hex') AS stake_addr_hash,
       sr.cert_index AS cert_index,
       block.slot_no AS slot_no,
       tx.block_index AS txidx
FROM public.stake_registration sr
         JOIN public.stake_address sa ON sr.addr_id = sa.id
         JOIN public.tx ON sr.tx_id = tx.id
         JOIN public.block ON tx.block_id = block.id
ORDER BY block.epoch_no, block.slot_no, tx.block_index, encode(sa.hash_raw, 'hex'), sr.cert_index ASC;

ALTER TABLE analytics.vw_bq_stake_registration
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_stake_registration TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_stake_registration TO db_sync_master;
