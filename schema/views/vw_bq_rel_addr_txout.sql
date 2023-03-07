-- FUNCTION: analytics.vw_bq_rel_addr_txout(integer)

-- DROP FUNCTION IF EXISTS analytics.vw_bq_rel_addr_txout(integer);

CREATE OR REPLACE FUNCTION analytics.vw_bq_rel_addr_txout(
    epoch_number integer)
    RETURNS TABLE(epoch_no word31type, address character varying,
				  address_has_script boolean, payment_cred text, data_hash text, inline_datum text, reference_script text,
				  outputs json)
    LANGUAGE 'plpgsql'
    COST 100
    STABLE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

BEGIN
    RETURN QUERY
        WITH dat AS
                 (SELECT block.epoch_no AS _epoch_no,
                         tx_out.address AS _address,
                         tx_out.address_has_script AS _addr_scr,
                         COALESCE(encode(tx_out.payment_cred,'hex'),NULL) AS _paym_cred,
                         COALESCE(encode(tx_out.data_hash,'hex'),NULL) AS _data_hash,
                         COALESCE(encode(datum.hash,'hex'),NULL) AS _inline_datum,
                         COALESCE(encode(script.hash,'hex'),NULL) AS _inline_script,
                         block.slot_no,
                         tx.block_index AS txidx,
                         tx_out.index   AS idx,
                         (row_number() OVER (PARTITION BY block.epoch_no, tx_out.address
                                             ORDER BY block.slot_no, tx.block_index, tx_out.index) - 1) / 1000::INTEGER AS cluster_no   -- zero indexed
                  FROM public.tx_out
                  JOIN public.tx ON tx.id = tx_out.tx_id
                  JOIN public.block ON block.id = tx.block_id
                  LEFT JOIN public.datum ON datum.id = tx_out.inline_datum_id
                  LEFT JOIN public.script ON script.id = tx_out.reference_script_id
                  WHERE block.epoch_no = epoch_number
                  ORDER BY block.epoch_no, tx_out.address, block.slot_no, tx.block_index, tx_out.index ASC
                 )
        SELECT _epoch_no,
               _address,
               _addr_scr,
               _paym_cred,
               _data_hash,
               _inline_datum,
               _inline_script,
               json_agg(
                       json_build_object('idx', idx,
                                         'slot_no', slot_no,
                                         'txidx', txidx)
                       order by slot_no, txidx, idx ASC
                   ) AS _outputs
        FROM dat
        GROUP BY _epoch_no, _address, _addr_scr, _paym_cred, _data_hash, _inline_datum, _inline_script, cluster_no
        ORDER BY _epoch_no, _address ASC;
END;
$BODY$;

ALTER FUNCTION analytics.vw_bq_rel_addr_txout(integer)
    OWNER TO db_sync_master;

GRANT EXECUTE ON FUNCTION analytics.vw_bq_rel_addr_txout(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION analytics.vw_bq_rel_addr_txout(integer) TO db_sync_master;

GRANT EXECUTE ON FUNCTION analytics.vw_bq_rel_addr_txout(integer) TO db_sync_reader;
