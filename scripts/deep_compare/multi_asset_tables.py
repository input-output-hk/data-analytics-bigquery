def query_multi_asset_tables(epoch_no):
    return [query_ma_minting(epoch_no)]


def query_ma_minting(epoch_no):
    return (
            f"""SELECT TO_BASE64(SHA256(innerq.hash_b64)) AS hash_b64 FROM
                (SELECT STRING_AGG(TO_BASE64(SHA256(str)), ',') AS hash_b64 FROM
                 (SELECT
                       '('|| (fingerprint)
                       ||',' || (policyid)
                       ||',' || (TO_BASE64(name_bytes))
                       ||',' || (epoch_no)
                       ||',' || TO_JSON_STRING(minting)
                       ||')' AS str
                  FROM
                  (SELECT fingerprint, policyid, name_bytes, epoch_no, minting
                     FROM `iog-data-analytics.cardano_mainnet.ma_minting` 
                     WHERE epoch_no = {epoch_no}
                     ORDER BY fingerprint ASC))
                ) AS innerq;""",
            f"""WITH dat AS
                  (SELECT fingerprint, policyid, name_bytes, epoch_no, minting
                     FROM analytics.vw_bq_ma_minting 
                     WHERE epoch_no = {epoch_no})
                SELECT encode(SHA256(innerq.hash_b64),'base64') AS hash_b64 FROM
                (SELECT STRING_AGG(encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n]', '', 'g'), '[\s]', '', 'g')::bytea),'base64'), ',')::bytea AS hash_b64 FROM
                 (SELECT 
                       '('|| (fingerprint)
                       ||',' || (policyid)
                       ||',' || (name_bytes)
                       ||',' || (epoch_no)
                       ||',' || (minting::text)
                       ||')' AS str
                  FROM dat) AS subq
                ) AS innerq;
                """,
            lambda x: x, lambda x: x)
