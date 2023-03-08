-- View: analytics.vw_bq_ma_minting

-- DROP VIEW analytics.vw_bq_ma_minting;

CREATE OR REPLACE VIEW analytics.vw_bq_ma_minting
 AS
 SELECT ma.fingerprint,
    encode(ma.policy::bytea, 'hex'::text) AS policyid,
    encode(ma.name::bytea, 'base64'::text) AS name_bytes,
    subq.epoch_no,
    subq.minting
   FROM ( SELECT mint.ident,
            b.epoch_no,
            json_agg(('{"quantity":'||mint.quantity::text||',"slot_no":'||b.slot_no::text||',"txidx":'||tx.block_index::text||'}')::json order by b.slot_no, tx.block_index ASC) AS minting
           FROM ma_tx_mint mint
             JOIN tx ON tx.id = mint.tx_id
             JOIN block b ON b.id = tx.block_id
          GROUP BY b.epoch_no, mint.ident) subq
     JOIN multi_asset ma ON ma.id = subq.ident
  ORDER BY ma.fingerprint;

ALTER TABLE analytics.vw_bq_ma_minting
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_ma_minting TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_ma_minting TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_ma_minting TO db_sync_master;

