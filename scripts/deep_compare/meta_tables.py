def query_meta_tables():
    return [
            query_meta(),
            query_schema_version()
    ]


def query_meta():
    return (f"""SELECT TO_BASE64(SHA256(innerq.str)) AS hash_b64 FROM
                 (SELECT
                        '('|| (start_time)
                       ||',' || (network_name)
                       ||',' || (version)
                       ||')' AS str
                  FROM cardano_mainnet.meta 
                ) AS innerq;""",
            f"""SELECT encode(SHA256(subq.str::bytea),'base64') AS hash_b64 FROM
                    (SELECT 
                           '('|| (start_time)
                           ||',' || (network_name)
                           ||',' || (version)
                           ||')' AS str
                      FROM public.meta) AS subq""",
            lambda x: x, lambda x: x)


def query_schema_version():
    return (f"""SELECT TO_BASE64(SHA256(innerq.str)) AS hash_b64 FROM
                 (SELECT
                        '('||
                       (stage_one)
                       ||',' || (stage_two)
                       ||',' || (stage_three)
                       ||')' AS str
                  FROM cardano_mainnet.schema_version 
                ) AS innerq;""",
            f"""SELECT encode(SHA256(regexp_replace(regexp_replace(subq.str, '[\n+]', '', 'g'), '[\s+]', '', 'g')::bytea),'base64') AS hash_b64 FROM
                    (SELECT 
                           '('|| (stage_one)
                           ||',' || (stage_two)
                           ||',' || (stage_three)
                           ||')' AS str
                      FROM public.schema_version) AS subq""",
            lambda x: x, lambda x: x)
