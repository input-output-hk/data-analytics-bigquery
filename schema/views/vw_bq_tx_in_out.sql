-- View: analytics.vw_bq_tx_in_out

-- DROP VIEW analytics.vw_bq_tx_in_out;

CREATE OR REPLACE VIEW analytics.vw_bq_tx_in_out
AS
SELECT block.epoch_no,
       block.slot_no,
       tx.block_index AS txidx,
       ( SELECT json_agg(subq.*)::text AS json_agg
         FROM ( SELECT tx_in.tx_out_index AS in_idx,
                       blockin.slot_no AS in_slot_no,
                       txin.block_index AS in_txidx
                FROM tx_in
                         JOIN tx txin ON txin.id = tx_in.tx_out_id
                         JOIN block blockin ON blockin.id = txin.block_id
                WHERE tx_in.tx_in_id = tx.id
                ORDER BY blockin.slot_no, txin.block_index) subq) AS inputs,
       ( SELECT json_agg(subq.*)::text AS json_agg
         FROM ( SELECT tx_out.address AS out_address,
                       tx_out.index AS out_idx,
                       ( SELECT array_agg(subq2.*) AS array_agg
                         FROM ( SELECT ma.fingerprint,
                                       maout.quantity
                                FROM ma_tx_out maout
                                         JOIN multi_asset ma ON ma.id = maout.ident
                                WHERE maout.tx_out_id = tx_out.id
                                ORDER BY ma.fingerprint, maout.quantity) subq2) AS out_ma,
                       tx_out.value AS out_value
                FROM tx_out
                WHERE tx_out.tx_id = tx.id
                ORDER BY tx_out.index) subq) AS outputs
FROM tx
         JOIN block ON block.id = tx.block_id
ORDER BY block.epoch_no, block.slot_no, tx.block_index;

ALTER TABLE analytics.vw_bq_tx_in_out
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_tx_in_out TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_tx_in_out TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_tx_in_out TO db_sync_master;

