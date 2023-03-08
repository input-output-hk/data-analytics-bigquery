import json
import base64
from decimal import Decimal

def query_pool_tables(epoch_no):
    return [
            query_pool_offline_data(epoch_no),
            query_pool_owner(epoch_no),
            query_pool_retire(epoch_no),
            query_pool_update(epoch_no)
    ]


def query_pool_offline_data(epoch_no):
    return (  f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (pool_hash)
                ||',' || (epoch_no)
                ||',' || (ticker_name)
                ||',' || (TO_JSON_STRING(json))
                ||',' || (metadata_url)
                ||',' || (metadata_hash)
                ||',' || (metadata_registered_tx_hash)
                ||')' AS str
              FROM
              (SELECT pool_hash, epoch_no, ticker_name, json, metadata_url, metadata_hash, metadata_registered_tx_hash
                 FROM `iog-data-analytics.cardano_mainnet.pool_offline_data` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, pool_hash, metadata_registered_tx_hash ASC))
            ) AS innerq;""",
              f"""WITH dat AS
                (SELECT pool_hash, epoch_no, ticker_name, json, metadata_url, metadata_hash, metadata_registered_tx_hash
                FROM analytics.vw_bq_pool_offline_data
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '"\s:\s"', '":"', 'g'), '",\s"', '","', 'g')::bytea), 'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (pool_hash)
                    ||',' || (epoch_no)
                    ||',' || (ticker_name)
                    ||',' || (json::text)
                    ||',' || (metadata_url)
                    ||',' || (metadata_hash)
                    ||',' || (metadata_registered_tx_hash)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
        lambda x: x, lambda x: x)

def query_pool_owner(epoch_no):
    return( f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (pool_hash)
                ||',' || (epoch_no)
                ||',' || (addr_hash)
                ||',' || (slot_no)
                ||',' || (txidx)
                ||')' AS str
              FROM
              (SELECT pool_hash, epoch_no, addr_hash, slot_no, txidx
                 FROM `iog-data-analytics.cardano_mainnet.pool_owner` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, txidx, pool_hash, addr_hash ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT pool_hash, epoch_no, addr_hash, slot_no, txidx
                FROM analytics.vw_bq_pool_owner
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(subq.str::bytea), 'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                  '('|| (pool_hash)
                    ||',' || (epoch_no)
                    ||',' || (addr_hash)
                    ||',' || (slot_no)
                    ||',' || (txidx)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
                lambda x: x, lambda x: x)

def query_pool_retire(epoch_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (pool_hash)
                ||',' || (retiring_epoch)
                ||',' || (epoch_no)
                ||',' || (cert_index)
                ||',' || (announced_tx_hash)
                ||',' || (slot_no)
                ||',' || (announced_txidx)
                ||')' AS str
              FROM
              (SELECT pool_hash, retiring_epoch, epoch_no, cert_index, announced_tx_hash, slot_no, announced_txidx
                 FROM `iog-data-analytics.cardano_mainnet.pool_retire` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, announced_txidx, pool_hash ASC))
            ) AS innerq;""",
           f"""WITH dat AS
                (SELECT pool_hash, retiring_epoch, epoch_no, cert_index, announced_tx_hash, slot_no, announced_txidx
                FROM analytics.vw_bq_pool_retire
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(subq.str::bytea), 'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (pool_hash)
                     ||',' || (retiring_epoch)
                     ||',' || (epoch_no)
                     ||',' || (cert_index)
                     ||',' || (announced_tx_hash)
                     ||',' || (slot_no)
                     ||',' || (announced_txidx)
                     ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
    lambda x: x, lambda x: x)

def query_pool_update(epoch_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (active_epoch_no)
                ||',' || (pool_hash)
                ||',' || (cert_index)
                ||',' || (vrf_key_hash)
                ||',' || (pledge)
                ||',' || (reward_addr)
                ||',' || (margin)
                ||',' || (fixed_cost)
                ||',' || (registered_tx_hash)
                ||',' || (epoch_no)
                ||',' || (metadata_url)
                ||',' || (metadata_hash)
                ||',' || (metadata_registered_tx_hash)
                ||')' AS str
              FROM
              (SELECT active_epoch_no, pool_hash, cert_index, vrf_key_hash, pledge, reward_addr, margin, fixed_cost, registered_tx_hash, epoch_no, metadata_url, metadata_hash, metadata_registered_tx_hash
                 FROM `iog-data-analytics.cardano_mainnet.pool_update` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, pool_hash, registered_tx_hash, cert_index ASC))
            ) AS innerq;""",
        f"""WITH dat AS
                (SELECT active_epoch_no, pool_hash, cert_index, vrf_key_hash, pledge, reward_addr, margin, fixed_cost, registered_tx_hash, epoch_no, metadata_url, metadata_hash, metadata_registered_tx_hash
                FROM analytics.vw_bq_pool_update
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(subq.str, '\\\\', '\\\\\\\\', 'g')::bytea), 'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (active_epoch_no)
                    ||',' || (pool_hash)
                    ||',' || (cert_index)
                    ||',' || (vrf_key_hash)
                    ||',' || (pledge)
                    ||',' || (reward_addr)
                    ||',' || (margin::real)
                    ||',' || (fixed_cost)
                    ||',' || (registered_tx_hash)
                    ||',' || (epoch_no)
                    ||',' || (metadata_url)
                    ||',' || (metadata_hash)
                    ||',' || (metadata_registered_tx_hash)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
    lambda x: x, lambda x: x)
