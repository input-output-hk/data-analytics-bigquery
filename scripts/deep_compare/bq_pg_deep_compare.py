import os
import re

import pandas as pd
import pandas_gbq
import sys
import psycopg2
from google.oauth2 import service_account
from datetime import datetime

from block_tables import query_block_tables
from epoch_tables import query_epoch_tables
from meta_tables import query_meta_tables
from multi_asset_tables import query_multi_asset_tables
from pool_tables import query_pool_tables
from staking_tables import query_staking_tables
from script_tables import query_script_tables
from tx_tables import query_tx_tables

def bq_pg_queries(epoch_no, epoch_start_slot_no, epoch_end_slot_no):
    return query_script_tables(epoch_no) + \
           query_epoch_tables(epoch_no) + \
           query_block_tables(epoch_no) + \
           query_staking_tables(epoch_no) + \
           query_tx_tables(epoch_no, epoch_start_slot_no, epoch_end_slot_no) + \
           query_multi_asset_tables(epoch_no) + \
           query_pool_tables(epoch_no) + \
           query_meta_tables()


def get_pg_connection():
    try:
        connection = psycopg2.connect(dbname=os.environ['PGDATABASE'], host=os.environ['PGHOST'],
                                      port=os.environ['PGPORT'],
                                      password=os.environ['PGPASSWORD'], user=os.environ['PGUSER'])
        return connection
    except Exception as e:
        print("Failed connecting to database. Cause - %s" % (str(e)))
        exit(1)


def get_pg(pg_conn, pg_query):
    try:
        query_res = pd.read_sql_query(pg_query, pg_conn)
        return pd.DataFrame(query_res)
    except psycopg2.Error as e:
        print(f'Εrror executing sql query: {pg_query}', e)


def get_bq(creds, bq_query):
    proj_id = os.environ['BQPROJECT']
    df = pandas_gbq.read_gbq(bq_query, project_id=proj_id, location="europe-west6", credentials=creds)
    if 'invalid_after' in df.columns:
        df["invalid_after"] = pd.to_numeric(df["invalid_after"])
    if 'monetary_expand_rate' in df.columns:
        df["monetary_expand_rate"] = pd.to_numeric(df["monetary_expand_rate"])
        df["influence"] = pd.to_numeric(df["influence"])
        df["treasury_growth_rate"] = pd.to_numeric(df["treasury_growth_rate"])
        df["price_mem"] = pd.to_numeric(df["price_mem"])
        df["price_step"] = pd.to_numeric(df["price_step"])
    return df


def get_last_epoch(cur):
    try:
        cur.execute("SELECT max(epoch_no) FROM public.block;")
        return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f'Εrror retrieving last epoch_no', e)

def get_epoch_start_slot(cur, epoch_no):
    try:
        cur.execute(f"SELECT min(slot_no) FROM public.block WHERE epoch_no={epoch_no};")
        return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f"Εrror retrieving epoch's start slot", e)

def get_epoch_end_slot(cur, epoch_no):
    try:
        cur.execute(f"SELECT max(slot_no) FROM public.block WHERE epoch_no={epoch_no};")
        return cur.fetchone()[0]
    except psycopg2.Error as e:
        print(f"Εrror retrieving epoch's start slot", e)

def get_bq_max_slot(credentials):
    df = get_bq(credentials, "SELECT MAX(slot_no) FROM `iog-data-analytics.cardano_mainnet.block`;")
    return df.iloc[0, 0]

def log_deep_comparison(credentials, epoch_no, table_name0, tstart, tend, pg_query0, bq_query0, pg_hash0, bq_hash0):
    table_name = table_name0.replace('`','')
    pg_query = pg_query0.replace('\\','\\\\').replace('"','\\"').replace('\n','\\n')
    bq_query = bq_query0.replace('\\','\\\\').replace('"','\\"').replace('\n','\\n')
    pg_hash = 'empty' if pg_hash0.empty else pg_hash0["hash_b64"][0]
    bq_hash = 'empty' if bq_hash0.empty else bq_hash0["hash_b64"][0]
    query = f"INSERT INTO db_sync.log_deep_comparison \
                (epoch_no,table_name,start_time,end_time,pg_query,bq_query,pg_hash,bq_hash) \
              VALUES \
                ({epoch_no},'{table_name}','{tstart.isoformat()}','{tend.isoformat()}',\"{pg_query}\",\"{bq_query}\",'{pg_hash}','{bq_hash}') "
    print(f"query = \n{query}")
    get_bq(credentials, query)

def main():
    con = get_pg_connection()
    cur = con.cursor()
    credentials = service_account.Credentials.from_service_account_file(
            '/usr/src/app/scripts/key.json',
    )
    # read epoch number from argument if provided
    epoch_no = sys.argv[1] if len(sys.argv) == 2 else get_last_epoch(cur) - 1
    print(f"Running BigQuery/Postgres deep comparison for epoch: {epoch_no}")
    epoch_start_slot_no = get_epoch_start_slot(cur, epoch_no)
    epoch_end_slot_no = get_epoch_end_slot(cur, epoch_no)
    queries = bq_pg_queries(epoch_no, epoch_start_slot_no, epoch_end_slot_no)
    with open('msg.txt', 'w') as f:
        f.write(f"Running BigQuery/Postgres deep comparison for epoch: {epoch_no}\n")
        for bq, pg, bq_post_process, pg_post_process in queries:
            tstart = datetime.utcnow()
            res = bq.rfind("FROM ")
            sub = bq[res + 5:]
            next_space_idx = re.search('\s', sub)
            table_name = sub[0:next_space_idx.start()] if next_space_idx else sub
            print(f"Table: {table_name}")
            pg_df = pg_post_process(get_pg(con, pg))
            print(f"Pg returned: {pg_df}")
            bq_df = bq_post_process(get_bq(credentials, bq))
            print(f"BQ returned: {bq_df}")
            f.write(f"{table_name} -- PG #rows: {len(pg_df.index)} - BQ #rows: {len(bq_df.index)}\n")
            diff = pg_df.compare(bq_df)  ## I think this one is currently too cheap unless we have the data with a strict order in BQ! in the meantime: we need a row by row and col by col comparison with JSON unpacking and sorting
            diff_msg = "PG - BQ have identical contents\n\n" if (len(diff.index) == 0) else f"PG - BQ contents differ:\n{diff}\n\n"
            f.write(diff_msg)
            tend = datetime.utcnow()
            log_deep_comparison(credentials, epoch_no, table_name, tstart, tend, pg, bq, pg_df, bq_df)
        try:
            cur.close()
            con.commit()
        except psycopg2.Error as e:
            print(f"Error in closing database connection: {e}")
            con.rollback()
            con.close()
            exit(1)
        print("Finished running BigQuery/Postgres deep comparison")


# %%
if __name__ == '__main__':
    main()
