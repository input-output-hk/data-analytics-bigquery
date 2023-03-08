-- FUNCTION: analytics.vw_bq_rel_stake_txout(integer)

-- DROP FUNCTION IF EXISTS analytics.vw_bq_rel_stake_txout(integer);

CREATE OR REPLACE FUNCTION analytics.vw_bq_rel_stake_txout(
    epoch_number integer)
    RETURNS TABLE(epoch_no bigint, address character varying, outputs json)
    LANGUAGE 'plpgsql'
    COST 100
    STABLE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

BEGIN
    RETURN QUERY
        with dat AS
                 (SELECT block.epoch_no AS _epoch_no,
                         sa.view        AS _address,
                         block.slot_no,
                         tx.block_index AS txidx,
                         tx_out.index   AS idx,
                         (row_number() OVER (PARTITION BY block.epoch_no, sa.view ORDER BY block.slot_no, tx.block_index, tx_out.index) - 1) / 1000::integer AS cluster_no
                  FROM public.tx_out
                           JOIN public.tx ON tx.id = tx_out.tx_id
                           JOIN public.block ON block.id = tx.block_id
                           JOIN public.stake_address AS sa ON sa.id = tx_out.stake_address_id
                  WHERE block.epoch_no = epoch_number
                  ORDER BY block.epoch_no, sa.view, block.slot_no, tx.block_index, tx_out.index ASC
                 )
        SELECT cast(_epoch_no as bigint) AS epoch_no,
               _address AS address,
               json_agg(
                       json_build_object('idx', idx,
                                         'slot_no', slot_no,
                                         'txidx', txidx)
                       order by slot_no, txidx, idx ASC
                   ) as outputs
        FROM dat
        GROUP BY _epoch_no, _address, cluster_no
        ORDER BY _epoch_no, _address ASC;
END;
$BODY$;

ALTER FUNCTION analytics.vw_bq_rel_stake_txout(integer)
    OWNER TO db_sync_master;

GRANT EXECUTE ON FUNCTION analytics.vw_bq_rel_stake_txout(integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION analytics.vw_bq_rel_stake_txout(integer) TO db_sync_master;

GRANT EXECUTE ON FUNCTION analytics.vw_bq_rel_stake_txout(integer) TO db_sync_reader;

