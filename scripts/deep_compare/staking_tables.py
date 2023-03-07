def query_staking_tables(epoch_no):
    return [
            query_stake_registration(epoch_no),
            query_stake_deregistration(epoch_no),
            query_reward(epoch_no),
            query_withdrawal(epoch_no),
            query_delegation(epoch_no)
    ]


def query_stake_registration(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (slot_no)
                ||',' || (txidx)
                ||',' || (stake_addr_hash)
                ||',' || (cert_index)
                ||')' AS str
              FROM
              (SELECT epoch_no, slot_no, txidx, stake_addr_hash, cert_index
                 FROM `iog-data-analytics.cardano_mainnet.stake_registration` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, txidx, stake_addr_hash, cert_index ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, slot_no, txidx, stake_addr_hash, cert_index
                FROM analytics.vw_bq_stake_registration
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (slot_no)
                    ||',' || (txidx)
                    ||',' || (stake_addr_hash)
                    ||',' || (cert_index)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)


def query_stake_deregistration(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (slot_no)
                ||',' || (txidx)
                ||',' || (stake_addr_hash)
                ||',' || (cert_index)
                ||')' AS str
              FROM
              (SELECT epoch_no, slot_no, txidx, stake_addr_hash, cert_index
                 FROM `iog-data-analytics.cardano_mainnet.stake_deregistration` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, txidx, stake_addr_hash, cert_index ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, slot_no, txidx, stake_addr_hash, cert_index
                FROM analytics.vw_bq_stake_deregistration
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (slot_no)
                    ||',' || (txidx)
                    ||',' || (stake_addr_hash)
                    ||',' || (cert_index)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)


def query_reward(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (stake_addr_hash)
                ||',' || (type)
                ||',' || (amount)
                ||',' || (earned_epoch)
                ||',' || (pool_hash)
                ||')' AS str
              FROM
              (SELECT epoch_no, stake_addr_hash, type, amount, earned_epoch, pool_hash
                 FROM `iog-data-analytics.cardano_mainnet.reward` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, stake_addr_hash, type, pool_hash ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, stake_addr_hash, type, amount, earned_epoch, pool_hash
                FROM analytics.vw_bq_reward
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (stake_addr_hash)
                    ||',' || (type)
                    ||',' || (amount)
                    ||',' || (earned_epoch)
                    ||',' || (pool_hash)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)

def query_withdrawal(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (stake_addr_hash)
                ||',' || (amount)
                ||',' || (slot_no)
                ||',' || (txidx)
                ||')' AS str
              FROM
              (SELECT epoch_no, stake_addr_hash, amount, slot_no, txidx
                 FROM `iog-data-analytics.cardano_mainnet.withdrawal` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, txidx, stake_addr_hash ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, stake_addr_hash, amount, slot_no, txidx
                FROM analytics.vw_bq_withdrawal
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (stake_addr_hash)
                    ||',' || (amount)
                    ||',' || (slot_no)
                    ||',' || (txidx)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)


def query_delegation(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (stake_addr_hash)
                ||',' || (TO_JSON_STRING(delegations))
                ||')' AS str
              FROM
              (SELECT epoch_no, stake_addr_hash, delegations
                 FROM `iog-data-analytics.cardano_mainnet.delegation` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, stake_addr_hash ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, stake_addr_hash, delegations
                FROM analytics.vw_bq_delegation
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (stake_addr_hash)
                    ||',' || (delegations::text)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)