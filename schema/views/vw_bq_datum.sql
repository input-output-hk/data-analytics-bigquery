-- View: analytics.vw_bq_datum

-- DROP VIEW analytics.vw_bq_datum;

CREATE OR REPLACE VIEW analytics.vw_bq_datum
 AS
 SELECT block.epoch_no,
    block.slot_no,
    tx.block_index AS txidx,
    encode(datum.hash::bytea, 'hex'::text) AS datum_hash,
    encode(datum.bytes, 'base64'::text) AS bytes,
    datum.value
   FROM datum
     JOIN tx ON tx.id = datum.tx_id
     JOIN block ON block.id = tx.block_id
  ORDER BY block.slot_no;

ALTER TABLE analytics.vw_bq_datum
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_datum TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_datum TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_datum TO db_sync_master;
