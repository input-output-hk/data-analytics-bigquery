-- View: analytics.vw_bq_block_hash

-- DROP VIEW analytics.vw_bq_block_hash;

CREATE OR REPLACE VIEW analytics.vw_bq_block_hash
AS
SELECT block.epoch_no,
       block.slot_no,
       encode(block.hash::bytea, 'hex'::text) AS block_hash
FROM block
ORDER BY block.epoch_no, block.slot_no ASC;

ALTER TABLE analytics.vw_bq_block_hash
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_block_hash TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_block_hash TO db_sync_master;

