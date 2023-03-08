-- View: analytics.vw_bq_collateral

-- DROP VIEW analytics.vw_bq_collateral;

CREATE OR REPLACE VIEW analytics.vw_bq_collateral
AS
SELECT blin.epoch_no,
       blin.slot_no,
       txin.block_index  AS txidx,
       blout.epoch_no    AS epoch_no_out,
       blout.slot_no     AS slot_no_out,
       txout.block_index AS txidx_out,
       col.tx_out_index  AS tx_out_index
FROM public.collateral_tx_in col
         JOIN public.tx txin ON txin.id = col.tx_in_id
         JOIN public.block blin ON blin.id = txin.block_id
         JOIN public.tx txout ON txout.id = col.tx_out_id
         JOIN public.block blout ON blout.id = txout.block_id;

ALTER TABLE analytics.vw_bq_collateral
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_collateral TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_collateral TO db_sync_reader;
GRANT ALL ON TABLE analytics.vw_bq_collateral TO db_sync_master;
