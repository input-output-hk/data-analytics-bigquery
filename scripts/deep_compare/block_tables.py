def query_block_tables(epoch_no):
    return [
            query_block(epoch_no),
            query_block_hash(epoch_no)
    ]


def query_block(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (slot_no)
                ||',' || (block_time)
                ||',' || (block_size)
                ||',' || (tx_count)
                ||',' || (sum_tx_fee)
                ||',' || (script_count)
                ||',' || (sum_script_size)
                ||')' AS str
              FROM
              (SELECT epoch_no, slot_no, block_time, block_size, tx_count, sum_tx_fee, script_count, sum_script_size, pool_hash
                 FROM `iog-data-analytics.cardano_mainnet.block` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, slot_no, block_time, block_size, tx_count, sum_tx_fee, script_count, sum_script_size, pool_hash
                FROM analytics.vw_bq_block
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (slot_no)
                    ||',' || (block_time)
                    ||',' || (block_size)
                    ||',' || (tx_count)
                    ||',' || (sum_tx_fee)
                    ||',' || (script_count)
                    ||',' || (sum_script_size)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)


def query_block_hash(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
            (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
             (SELECT
                '('|| (epoch_no)
                ||',' || (slot_no)
                ||',' || (block_hash)
                ||')' AS str
              FROM
              (SELECT epoch_no, slot_no, block_hash
                 FROM `iog-data-analytics.cardano_mainnet.block_hash` 
                 WHERE epoch_no = {epoch_no}
                 ORDER BY epoch_no, slot_no ASC))
            ) AS innerq;""",
            f"""WITH dat AS
                (SELECT epoch_no, slot_no, block_hash
                FROM analytics.vw_bq_block_hash
                WHERE epoch_no = {epoch_no})
                
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                    '('|| (epoch_no)
                    ||',' || (slot_no)
                    ||',' || (block_hash)
                    ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;""",
            lambda x: x, lambda x: x)
