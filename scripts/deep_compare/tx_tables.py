import json
import ast

def query_tx_tables(epoch_no, epoch_start_slot_no, epoch_end_slot_no):
    return [
            query_tx(epoch_no),
            query_tx_in_out(epoch_no),
            query_tx_hash(epoch_no),
            query_rel_addr_txout(epoch_no),
            query_rel_stake_txout(epoch_no),
            query_rel_stake_hash(epoch_no),
            query_collateral(epoch_no),
            query_tx_metadata(epoch_no),
            query_tx_consumed_output(epoch_start_slot_no, epoch_end_slot_no)
    ]

def query_tx(epoch_no):
    return (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT
                           '('|| (epoch_no)
                           ||',' || (tx_hash)
                           ||',' || (block_time)
                           ||',' || (slot_no)
                           ||',' || (txidx)
                           ||',' || (out_sum)
                           ||',' || (fee)
                           ||',' || (deposit)
                           ||',' || (size)
                           ||',' || (COALESCE(CAST(invalid_before AS STRING), 'null'))
                           ||',' || (COALESCE(CAST(invalid_after AS STRING), 'null'))
                           ||',' || (valid_script)
                           ||',' || (script_size)
                           ||',' || (count_inputs)
                           ||',' || (count_outputs)
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, tx_hash, block_time, slot_no, txidx, out_sum, fee, deposit, size, invalid_before, invalid_after, valid_script, script_size, count_inputs, count_outputs
                         FROM `iog-data-analytics.cardano_mainnet.tx`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx ASC))
                    ) AS innerq;""",
            f"""WITH dat AS
                      (SELECT epoch_no, tx_hash, block_time, slot_no, txidx, out_sum, fee, deposit, size, invalid_before, invalid_after, valid_script, script_size, count_inputs, count_outputs
                         FROM analytics.vw_bq_tx
                         WHERE epoch_no = {epoch_no})
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| (epoch_no)
                           ||',' || (tx_hash)
                           ||',' || (block_time)
                           ||',' || (slot_no)
                           ||',' || (txidx)
                           ||',' || (out_sum)
                           ||',' || (fee)
                           ||',' || (deposit)
                           ||',' || (size)
                           ||',' || (COALESCE(invalid_before::text, 'null'))
                           ||',' || (COALESCE(invalid_after::text, 'null'))
                           ||',' || (valid_script)
                           ||',' || (script_size)
                           ||',' || (count_inputs)
                           ||',' || (count_outputs)
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq;""",
            lambda x: x, lambda x: x)

def query_tx_in_out(epoch_no):
    return  (f"""DROP TABLE IF EXISTS `iog-data-analytics.db_sync.tmp_tx_in_out`;
                 CREATE TABLE `iog-data-analytics.db_sync.tmp_tx_in_out` AS 
                 (SELECT RANK() OVER(ORDER BY epoch_no, slot_no, txidx ASC) row_number, 
                 TO_BASE64(SHA256( '('||
                           (epoch_no||','||slot_no||','||txidx)
                           ||',' || TO_JSON_STRING(inputs)
                           ||',' || COALESCE(TO_JSON_STRING(outputs), 'null')
                           ||')')) AS hash_b64
                         FROM `iog-data-analytics.cardano_mainnet.tx_in_out`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx ASC);
                  SELECT TO_BASE64(SHA256(innerq.agg_hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(hash_b64, ',') AS agg_hash_b64 FROM
                     `iog-data-analytics.db_sync.tmp_tx_in_out`
                    ) AS innerq;""",
             f"""WITH dat AS
                      (SELECT epoch_no, slot_no, txidx, inputs, outputs
                         FROM analytics.vw_bq_tx_in_out 
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx ASC)
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]+', '', 'g'), '[\s]+', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| (epoch_no||','||slot_no||','||txidx)
                           ||',' || (inputs::text)
                           ||',' || COALESCE(outputs::text, 'null')
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq;""",
             lambda x: x, lambda x: x)


def query_tx_hash(epoch_no):
    return  (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT
                           '('|| (epoch_no)
                           ||',' || (slot_no)
                           ||',' || (txidx)
                           ||',' || (tx_hash)
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, slot_no, txidx, tx_hash
                         FROM `iog-data-analytics.cardano_mainnet.tx_hash`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx ASC))
                    ) AS innerq;""",
             f"""WITH dat AS
                      (SELECT epoch_no, slot_no, txidx, tx_hash
                         FROM analytics.vw_bq_tx_hash 
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx ASC)
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| (epoch_no)
                           ||',' || (slot_no)
                           ||',' || (txidx)
                           ||',' || (tx_hash)
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq;""",
             lambda x: x, lambda x: x)


def query_rel_stake_txout(epoch_no):
    return  (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           ||',' || address
                           ||',' || TO_JSON_STRING(outputs)
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, address, outputs
                         FROM `iog-data-analytics.cardano_mainnet.rel_stake_txout`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, UPPER(address), JSON_VALUE(outputs[0].slot_no), JSON_VALUE(outputs[0].txidx), JSON_VALUE(outputs[0].idx) ASC))   -- BigQuery sorting is case sensitive compared to Postgres
                    ) AS innerq;""",
             f"""WITH dat AS
                      (SELECT epoch_no, address, outputs
                       FROM analytics.vw_bq_rel_stake_txout({epoch_no})
                       ORDER BY epoch_no, address, (outputs->0->>'slot_no')::integer, (outputs->0->>'txidx')::integer, (outputs->0->>'idx')::integer ASC)  
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           ||',' || address
                           ||',' || outputs::text
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq""",
             lambda x: x, lambda x: x)

def query_rel_stake_hash(epoch_no):
    return  (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           || ',' || slot_no
                           ||',' || stake_address
                           ||',' || stake_addr_hash
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, slot_no, stake_address, stake_addr_hash
                         FROM `iog-data-analytics.cardano_mainnet.rel_stake_hash`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, stake_address ASC))
                    ) AS innerq;""",
             f"""WITH dat AS
                      (SELECT epoch_no, slot_no, stake_address, stake_addr_hash
                       FROM analytics.vw_bq_rel_stake_hash
                       WHERE epoch_no = {epoch_no})
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           || ',' ||slot_no
                           ||',' || stake_address
                           ||',' || stake_addr_hash
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq;""",
             lambda x: x, lambda x: x)


def query_rel_addr_txout(epoch_no):
    return  (f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           ||',' || address
                           ||',' || TO_JSON_STRING(outputs)
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, address, outputs
                         FROM `iog-data-analytics.cardano_mainnet.rel_addr_txout`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, UPPER(address), CAST(JSON_VALUE(outputs[0].slot_no) AS INT64), CAST(JSON_VALUE(outputs[0].txidx) AS INT64), CAST(JSON_VALUE(outputs[0].idx) AS INT64) ASC))   -- BigQuery sorting is case sensitive compared to Postgres
                    ) AS innerq;""",
             f"""WITH dat AS
                      (SELECT epoch_no, address, outputs
                       FROM analytics.vw_bq_rel_addr_txout({epoch_no})
                       ORDER BY epoch_no, address, (outputs->0->>'slot_no')::integer, (outputs->0->>'txidx')::integer, (outputs->0->>'idx')::integer ASC)
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           ||',' || address
                           ||',' || outputs::text
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq""",
             lambda x: x, lambda x: x)


def query_collateral(epoch_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT 
                           '('|| epoch_no 
                           ||',' || slot_no
                           ||',' || txidx
                           ||',' || epoch_no_out
                           ||',' || slot_no_out
                           ||',' || txidx_out
                           ||',' || tx_out_index                       
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, slot_no, txidx, epoch_no_out, slot_no_out, txidx_out, tx_out_index	
                         FROM `iog-data-analytics.cardano_mainnet.collateral`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx, epoch_no_out, slot_no_out, txidx_out, tx_out_index ASC))
                    ) AS innerq;""",
            f"""WITH dat AS
                      (SELECT epoch_no, slot_no, txidx, epoch_no_out, slot_no_out, txidx_out, tx_out_index
                       FROM analytics.vw_bq_collateral
                       WHERE epoch_no = {epoch_no}
                       ORDER BY epoch_no, slot_no, txidx, epoch_no_out, slot_no_out, txidx_out, tx_out_index ASC)
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           ||',' || slot_no
                           ||',' || txidx
                           ||',' || epoch_no_out
                           ||',' || slot_no_out
                           ||',' || txidx_out
                           ||',' || tx_out_index                       
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq""",
                lambda x: x, lambda x: x)


# We excluded the metadata field as it can contain arbitrary fields that in BigQuery are stored alphabetically
def query_tx_metadata(epoch_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT 
                           '('|| epoch_no 
                           ||',' || slot_no
                           ||',' || txidx
                           ||',' || tx_hash
                           ||')' AS str
                      FROM
                      (SELECT epoch_no, slot_no, txidx, tx_hash	
                         FROM `iog-data-analytics.cardano_mainnet.tx_metadata`
                         WHERE epoch_no = {epoch_no}
                         ORDER BY epoch_no, slot_no, txidx, tx_hash ASC))
                    ) AS innerq;""",
           f"""WITH dat AS
                      (    SELECT block.epoch_no, encode(tx.hash,'hex') AS "tx_hash",
                block.slot_no, tx.block_index AS txidx, subq.metadata
            FROM (
                SELECT tx_id,
                    json_agg(('{{"index":'||key::text||',"meta":'||json::text||'}}')::json) AS metadata
                FROM public.tx_metadata
                JOIN public.tx itx ON itx.id = tx_id
                JOIN public.block ib ON ib.id = itx.block_id
                WHERE ib.epoch_no = {epoch_no}
                GROUP BY tx_id
                ORDER BY tx_id ASC
            ) AS subq
            JOIN public.tx ON tx.id = subq.tx_id
                                JOIN public.block ON block.id = tx.block_id AND block.epoch_no = {epoch_no}
                       ORDER BY block.epoch_no, block.slot_no, tx.block_index, encode(tx.hash,'hex') ASC)
                    
                    SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                           '('|| epoch_no 
                           ||',' || slot_no
                           ||',' || txidx
                           ||',' || tx_hash
                           ||')' AS str
                      FROM dat) AS subq
                    ) AS innerq""",
        lambda x: x, lambda x: x)

def query_tx_consumed_output(epoch_start_slot_no, epoch_end_slot_no):
    return(f"""SELECT TO_BASE64(SHA256(innerq0.hash_b64)) AS hash_b64 FROM
                    (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                     (SELECT 
                           '('|| slot_no
                           ||',' || txidx
                           ||',' || `index`
                           ||',' || consumed_in_slot_no
                           ||',' || consumed_in_txidx
                           ||')' AS str
                      FROM
                      (SELECT slot_no, txidx, `index`, consumed_in_slot_no, consumed_in_txidx	
                         FROM `iog-data-analytics.cardano_mainnet.tx_consumed_output`
                         WHERE consumed_in_slot_no >= {epoch_start_slot_no} AND consumed_in_slot_no <= {epoch_end_slot_no} AND mod(consumed_in_slot_no, 4) = 0
                         ORDER BY slot_no, txidx, `index` ASC))
                    ) AS innerq0
               UNION ALL 
               SELECT TO_BASE64(SHA256(innerq1.hash_b64)) AS hash_b64 FROM
                 (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                  (SELECT 
                        '('|| slot_no
                        ||',' || txidx
                        ||',' || `index`
                        ||',' || consumed_in_slot_no
                        ||',' || consumed_in_txidx
                        ||')' AS str
                   FROM
                   (SELECT slot_no, txidx, `index`, consumed_in_slot_no, consumed_in_txidx	
                      FROM `iog-data-analytics.cardano_mainnet.tx_consumed_output`
                      WHERE consumed_in_slot_no >= {epoch_start_slot_no} AND consumed_in_slot_no <= {epoch_end_slot_no} AND mod(consumed_in_slot_no, 4) = 1
                      ORDER BY slot_no, txidx, `index` ASC))
                 ) AS innerq1
                UNION ALL
                SELECT TO_BASE64(SHA256(innerq2.hash_b64)) AS hash_b64 FROM
                 (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                  (SELECT 
                        '('|| slot_no
                        ||',' || txidx
                        ||',' || `index`
                        ||',' || consumed_in_slot_no
                        ||',' || consumed_in_txidx
                        ||')' AS str
                   FROM
                   (SELECT slot_no, txidx, `index`, consumed_in_slot_no, consumed_in_txidx	
                      FROM `iog-data-analytics.cardano_mainnet.tx_consumed_output`
                      WHERE consumed_in_slot_no >= {epoch_start_slot_no} AND consumed_in_slot_no <= {epoch_end_slot_no} AND mod(consumed_in_slot_no, 4) = 2
                      ORDER BY slot_no, txidx, `index` ASC))
                 ) AS innerq2
               UNION ALL
               SELECT TO_BASE64(SHA256(innerq3.hash_b64)) AS hash_b64 FROM
                 (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                  (SELECT 
                        '('|| slot_no
                        ||',' || txidx
                        ||',' || `index`
                        ||',' || consumed_in_slot_no
                        ||',' || consumed_in_txidx
                        ||')' AS str
                   FROM
                   (SELECT slot_no, txidx, `index`, consumed_in_slot_no, consumed_in_txidx	
                      FROM `iog-data-analytics.cardano_mainnet.tx_consumed_output`
                      WHERE consumed_in_slot_no >= {epoch_start_slot_no} AND consumed_in_slot_no <= {epoch_end_slot_no} AND mod(consumed_in_slot_no, 4) = 3
                      ORDER BY slot_no, txidx, `index` ASC))
                 ) AS innerq3
                 ORDER BY hash_b64 ASC NULLS LAST;
                 """,
           f"""WITH dat0 AS
                      (SELECT slot_no, txidx, "index", consumed_in_slot_no, consumed_in_txidx
                       FROM analytics.vw_bq_tx_consumed_output
                       WHERE consumed_in_slot_no >= {epoch_start_slot_no} and consumed_in_slot_no <= {epoch_end_slot_no} AND consumed_in_slot_no % 4 = 0
                       ORDER BY slot_no, txidx, "index" ASC),
                dat1 AS
                      (SELECT slot_no, txidx, "index", consumed_in_slot_no, consumed_in_txidx
                       FROM analytics.vw_bq_tx_consumed_output
                       WHERE consumed_in_slot_no >= {epoch_start_slot_no} and consumed_in_slot_no <= {epoch_end_slot_no} AND consumed_in_slot_no % 4 = 1
                       ORDER BY slot_no, txidx, "index" ASC),
                dat2 AS
                      (SELECT slot_no, txidx, "index", consumed_in_slot_no, consumed_in_txidx
                       FROM analytics.vw_bq_tx_consumed_output
                       WHERE consumed_in_slot_no >= {epoch_start_slot_no} and consumed_in_slot_no <= {epoch_end_slot_no} AND consumed_in_slot_no % 4 = 2
                       ORDER BY slot_no, txidx, "index" ASC),
                dat3 AS
                      (SELECT slot_no, txidx, "index", consumed_in_slot_no, consumed_in_txidx
                       FROM analytics.vw_bq_tx_consumed_output
                       WHERE consumed_in_slot_no >= {epoch_start_slot_no} and consumed_in_slot_no <= {epoch_end_slot_no} AND consumed_in_slot_no % 4 = 3
                       ORDER BY slot_no, txidx, "index" ASC)
                SELECT hash_b64 FROM              
                    (SELECT encode(SHA256(innerq0.hash_b64),'base64') AS hash_b64 FROM
                    (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                     (SELECT
                            '('|| slot_no
                           ||',' || txidx
                           ||',' || "index"
                           ||',' || consumed_in_slot_no
                           ||',' || consumed_in_txidx
                           ||')' AS str
                          FROM dat0) AS subq
                        ) AS innerq0
                    UNION ALL
                    SELECT encode(SHA256(innerq1.hash_b64),'base64') AS hash_b64 FROM
                        (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                         (SELECT
                                '('|| slot_no
                               ||',' || txidx
                               ||',' || "index"
                               ||',' || consumed_in_slot_no
                               ||',' || consumed_in_txidx
                               ||')' AS str
                          FROM dat1) AS subq
                        ) AS innerq1
                    UNION ALL
                    SELECT encode(SHA256(innerq2.hash_b64),'base64') AS hash_b64 FROM
                        (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                         (SELECT
                                '('|| slot_no
                               ||',' || txidx
                               ||',' || "index"
                               ||',' || consumed_in_slot_no
                               ||',' || consumed_in_txidx
                               ||')' AS str
                          FROM dat2) AS subq
                        ) AS innerq2
                    UNION ALL
                    SELECT encode(SHA256(innerq3.hash_b64),'base64') AS hash_b64 FROM
                        (SELECT STRING_AGG(encode(SHA256(subq.str::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                         (SELECT
                                '('|| slot_no
                               ||',' || txidx
                               ||',' || "index"
                               ||',' || consumed_in_slot_no
                               ||',' || consumed_in_txidx
                               ||')' AS str
                          FROM dat3) AS subq
                        ) AS innerq3) AS hashes_union
                ORDER BY ASCII(SUBSTRING(hash_b64,1,1)),
                 ASCII(SUBSTRING(hash_b64,2,1)),
                 ASCII(SUBSTRING(hash_b64,3,1)),
                 ASCII(SUBSTRING(hash_b64,4,1)),
                 ASCII(SUBSTRING(hash_b64,5,1)),
                 ASCII(SUBSTRING(hash_b64,6,1)),
                 ASCII(SUBSTRING(hash_b64,7,1)),
                 ASCII(SUBSTRING(hash_b64,8,1)),
                 ASCII(SUBSTRING(hash_b64,9,1)),
                 ASCII(SUBSTRING(hash_b64,10,1)) ASC;""",
            lambda x: x, lambda x: x)
