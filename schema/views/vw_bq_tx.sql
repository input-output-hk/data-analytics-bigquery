-- View: analytics.vw_bq_tx

-- DROP VIEW analytics.vw_bq_tx;

CREATE OR REPLACE VIEW analytics.vw_bq_tx
AS
SELECT block.epoch_no,
       encode(tx.hash::bytea, 'hex'::text) AS tx_hash,
       block."time" AS block_time,
       block.slot_no,
       tx.block_index AS txidx,
       tx.out_sum,
       tx.fee,
       tx.deposit,
       tx.size,
       tx.invalid_before,
       tx.invalid_hereafter as invalid_after,
       tx.valid_contract as valid_script,
       tx.script_size,
       ( SELECT count(*) AS count
         FROM tx_in
         WHERE tx_in.tx_in_id = tx.id) AS count_inputs,
       ( SELECT count(*) AS count
         FROM tx_out
         WHERE tx_out.tx_id = tx.id) AS count_outputs
FROM tx
         JOIN block ON block.id = tx.block_id
ORDER BY block.epoch_no, block.slot_no, tx.block_index;

ALTER TABLE analytics.vw_bq_tx
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_tx TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_tx TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_tx TO db_sync_master;
