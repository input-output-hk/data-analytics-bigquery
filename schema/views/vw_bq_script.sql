-- View: analytics.vw_bq_script

-- DROP VIEW analytics.vw_bq_script;

CREATE OR REPLACE VIEW analytics.vw_bq_script
AS
SELECT block.epoch_no, 
       block.slot_no, 
       tx.block_index AS txidx,
       encode(sc.hash,'hex') AS script_hash, 
       sc.type::text AS type,
       sc.json AS json, 
       encode(sc.bytes, 'base64') AS bytes, 
       sc.serialised_size
FROM public.script sc
         JOIN public.tx ON tx.id = sc.tx_id
         JOIN public.block ON block.id = tx.block_id
ORDER BY block.epoch_no, block.slot_no, tx.block_index, script_hash ASC;

ALTER TABLE analytics.vw_bq_script
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_script TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_script TO db_sync_master;
