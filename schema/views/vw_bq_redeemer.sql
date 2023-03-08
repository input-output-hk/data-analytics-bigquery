-- View: analytics.vw_bq_redeemer

-- DROP VIEW analytics.vw_bq_redeemer;

CREATE OR REPLACE VIEW analytics.vw_bq_redeemer
 AS
 SELECT block.epoch_no::bigint AS "epoch_no",
    block.slot_no::bigint AS "slot_no",
    tx.block_index::bigint AS "txidx",
    count(*) AS "count",
    json_agg(json_build_object('byte', encode(redeemer_data.bytes, 'base64'::text),
                               'fee', red.fee, 'index', red.index, 'purpose', red.purpose,
                               'redeemer_data_value', redeemer_data.value,
                               'redeemer_hash', encode(redeemer_data.hash::bytea, 'hex'::text),
                               'script_hash', encode(red.script_hash::bytea, 'hex'::text),
                               'unit_mem', red.unit_mem, 'unit_steps', red.unit_steps)) AS "redeemers"
 FROM public.redeemer AS red
 JOIN public.tx ON tx.id = red.tx_id
 JOIN public.block ON block.id = tx.block_id
 JOIN public.redeemer_data ON redeemer_data.id = red.redeemer_data_id
 GROUP BY red.tx_id, block.epoch_no, block.slot_no, tx.block_index
 ORDER BY block.epoch_no, block.slot_no, tx.block_index;

ALTER TABLE analytics.vw_bq_redeemer
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_redeemer TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_redeemer TO db_sync_master;
