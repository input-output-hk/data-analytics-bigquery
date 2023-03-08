-- View: analytics.vw_bq_tx_consumed_output

-- DROP VIEW analytics.vw_bq_tx_consumed_output;

CREATE OR REPLACE VIEW analytics.vw_bq_tx_consumed_output
AS
SELECT b0.slot_no         AS slot_no,
       tx0.block_index    AS txidx,
       tx_in.tx_out_index AS index,
       b1.slot_no         AS consumed_in_slot_no,
       tx1.block_index    AS consumed_in_txidx
FROM public.tx_in
         JOIN public.tx AS tx0 ON tx_in.tx_out_id = tx0.id
         JOIN block AS b0 on tx0.block_id = b0.id
         JOIN public.tx AS tx1 ON tx_in.tx_in_id = tx1.id
         JOIN block AS b1 on tx1.block_id = b1.id
ORDER BY b0.slot_no, tx0.block_index, tx_in.tx_out_index;

ALTER TABLE analytics.vw_bq_tx_consumed_output
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_tx_consumed_output TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_tx_consumed_output TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_tx_consumed_output TO db_sync_master;

