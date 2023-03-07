-- View: analytics.vw_bq_pool_offline_data

-- DROP VIEW analytics.vw_bq_pool_offline_data;

CREATE OR REPLACE VIEW analytics.vw_bq_pool_offline_data
AS
SELECT
    encode(ph.hash_raw, 'hex') AS pool_hash,
    block.epoch_no AS epoch_no,
    pod.ticker_name AS ticker_name,
    json_build_object(
            'description', pod.json->'description',
            'homepage', pod.json->'homepage',
            'name', pod.json->'name',
            'ticker', pod.json->'ticker'
        ) AS json,
    pmr.url AS metadata_url,
    encode(pmr.hash, 'base64') AS metadata_hash,
    encode(tx.hash,'hex') AS metadata_registered_tx_hash
FROM public.pool_offline_data AS pod
         LEFT JOIN public.pool_metadata_ref pmr ON pod.pmr_id = pmr.id
         JOIN public.pool_hash ph ON pod.pool_id = ph.id
         JOIN tx ON pmr.registered_tx_id = tx.id
         JOIN block ON tx.block_id = block.id
ORDER BY block.epoch_no, pool_hash, metadata_registered_tx_hash ASC;

ALTER TABLE analytics.vw_bq_pool_offline_data
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_pool_offline_data TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_pool_offline_data TO db_sync_master;
