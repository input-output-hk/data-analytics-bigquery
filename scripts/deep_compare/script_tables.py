import json
import base64

def query_script_tables(epoch_no):
    return [
            query_redeemer(epoch_no),
            query_script(epoch_no)
    ]
    
def query_redeemer(epoch_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (slot_no)
                ||',' || (txidx)
                ||',' || (count)
                ||')' AS str
              FROM
              (SELECT epoch_no, slot_no, txidx, count, redeemers
                 FROM `iog-data-analytics.cardano_mainnet.redeemer` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, txidx  ASC))
            ) AS innerq;""",
           f"""WITH dat AS
                (SELECT epoch_no, slot_no, txidx, count, redeemers
                FROM analytics.vw_bq_redeemer
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (slot_no)
                    ||',' || (txidx)
                    ||',' || (count)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)


def query_script(epoch_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (slot_no)
                ||',' || (txidx)
                ||',' || (script_hash)
                ||',' || (`type`)
--                 ||',' || (COALESCE(TO_JSON_STRING(`json`), 'null')) json field needs re-ordering so skipping
                ||',' || (COALESCE(TO_BASE64(`bytes`), 'null'))
                ||',' || (COALESCE(CAST(serialised_size AS STRING), 'null'))
                ||')' AS str
              FROM
              (SELECT epoch_no, slot_no, txidx, script_hash, `type`, `json`, `bytes`, serialised_size
                 FROM `iog-data-analytics.cardano_mainnet.script` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no, txidx, script_hash ASC))
            ) AS innerq;""",
           f"""WITH dat AS
                (SELECT epoch_no, slot_no, txidx, script_hash, "type", "json", "bytes", serialised_size
                FROM analytics.vw_bq_script
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (slot_no)
                    ||',' || (txidx)
                    ||',' || (script_hash)
                    ||',' || ("type")
--                    ||',' || (COALESCE("json"::text), 'null')) json field needs re-ordering so skipping
                    ||',' || (COALESCE("bytes", 'null'))
                    ||',' || (COALESCE(serialised_size::text, 'null'))
                    ||')' AS str
                FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)
