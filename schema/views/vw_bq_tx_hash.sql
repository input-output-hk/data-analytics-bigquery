-- View: analytics.vw_bq_tx_hash

-- DROP VIEW analytics.vw_bq_tx_hash;

CREATE OR REPLACE VIEW analytics.vw_bq_tx_hash
AS
SELECT block.epoch_no,
       block.slot_no,
       tx.block_index AS txidx,
       encode(tx.hash::bytea, 'hex'::text) AS tx_hash
FROM tx
         JOIN block ON block.id = tx.block_id
ORDER BY block.epoch_no, block.slot_no, tx.block_index;

ALTER TABLE analytics.vw_bq_tx_hash
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_tx_hash TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_tx_hash TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_tx_hash TO db_sync_master;

