-- View: analytics.vw_bq_rel_stake_hash

-- DROP VIEW analytics.vw_bq_rel_stake_hash;

CREATE OR REPLACE VIEW analytics.vw_bq_rel_stake_hash
AS
SELECT b.epoch_no, 
       b.slot_no,
       sa.view AS stake_address,
       encode(sa.hash_raw, 'hex') AS stake_addr_hash
FROM public.stake_address sa
JOIN public.stake_registration sreg ON sreg.addr_id = sa.id
JOIN public.tx ON tx.id = sreg.tx_id
JOIN public.block b ON b.id = tx.block_id
ORDER BY b.epoch_no, b.slot_no, sa.view ASC;

ALTER TABLE analytics.vw_bq_rel_stake_hash
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_rel_stake_hash TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_rel_stake_hash TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_rel_stake_hash TO db_sync_master;

