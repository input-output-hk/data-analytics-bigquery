-- View: analytics.vw_bq_delegation

-- DROP VIEW analytics.vw_bq_delegation;

CREATE OR REPLACE VIEW analytics.vw_bq_delegation
AS
SELECT block.epoch_no as epoch_no,
       encode(stake_address.hash_raw, 'hex') as stake_addr_hash,
       json_agg(
               json_build_object(
                       'active_epoch_no', delegation.active_epoch_no,
                       'cert_index', delegation.cert_index,
                       'pool_hash', encode(pool_hash.hash_raw, 'hex'),
                       'slot_no', delegation.slot_no,
                       'txidx', tx.block_index
                   ) ORDER BY delegation.active_epoch_no, delegation.slot_no, tx.block_index ASC ) AS delegations
FROM public.delegation
         JOIN public.stake_address ON delegation.addr_id = stake_address.id
         JOIN pool_hash ON delegation.pool_hash_id = pool_hash.id
         JOIN tx ON delegation.tx_id = tx.id
         JOIN block ON tx.block_id = block.id
GROUP BY block.epoch_no, encode(stake_address.hash_raw, 'hex')
ORDER BY epoch_no, stake_addr_hash ASC;

ALTER TABLE analytics.vw_bq_delegation
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_delegation TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_delegation TO db_sync_master;
