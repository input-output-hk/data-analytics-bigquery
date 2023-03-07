-- View: analytics.vw_bq_block

-- DROP VIEW analytics.vw_bq_block;

CREATE OR REPLACE VIEW analytics.vw_bq_block
AS
SELECT block.epoch_no,
       block.slot_no,
       block."time" AS block_time,
       block.size AS block_size,
       block.tx_count,
       subq.sum_tx_fee,
       subq.script_count,
       subq.sum_script_size,
       encode(ph.hash_raw, 'hex')  as pool_hash
FROM ( SELECT block_1.id AS block_id,
              sum(tx.fee::numeric) AS sum_tx_fee,
              sum(tx.script_size::integer) AS sum_script_size, slot_leader_id,
              count(*) FILTER (WHERE tx.script_size::integer > 0 ) AS script_count
       FROM block block_1
                JOIN tx ON tx.block_id = block_1.id
       GROUP BY block_1.id, block_1.epoch_no) subq
         JOIN block ON block.id = subq.block_id
         JOIN public.slot_leader sl ON sl.id = subq.slot_leader_id 
        LEFT JOIN public.pool_hash ph ON ph.id = sl.pool_hash_id
ORDER BY block.epoch_no, block.slot_no ASC;

ALTER TABLE analytics.vw_bq_block
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_block TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_block TO db_sync_master;
