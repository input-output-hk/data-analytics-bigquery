-- View: analytics.vw_bq_reward

-- DROP VIEW analytics.vw_bq_reward;

CREATE OR REPLACE VIEW analytics.vw_bq_reward
AS
SELECT reward.spendable_epoch AS epoch_no,
       encode(stake_address.hash_raw, 'hex') as stake_addr_hash,
       reward.type::text AS type,
       reward.amount::bigint AS amount,
       reward.earned_epoch AS earned_epoch,
       encode(pool_hash.hash_raw, 'hex') AS pool_hash
FROM public.reward
         JOIN public.stake_address ON reward.addr_id = stake_address.id
         JOIN public.pool_hash ON reward.pool_id = pool_hash.id
ORDER BY reward.spendable_epoch, stake_addr_hash, reward.type, encode(pool_hash.hash_raw, 'hex') ASC;

ALTER TABLE analytics.vw_bq_reward
    OWNER TO db_sync_master;

GRANT SELECT ON TABLE analytics.vw_bq_reward TO PUBLIC;
GRANT ALL ON TABLE analytics.vw_bq_reward TO db_sync_master;
