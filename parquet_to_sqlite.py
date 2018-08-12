import argparse
import os
import pandas as pd
import sqlite3
import logging


def convert_table(source, table, conn):
    logging.warning(f'Reading {table}')
    df = pd.read_parquet(os.path.join(source, table))
    logging.warning(f'Writing {table}')
    df.to_sql(table, conn, index=False, if_exists='replace', index_label=[f'{table}_id'])
    logging.warning(f'Creating indexs for {table}')
    id_cols = [col for col in df.columns.tolist() if col.endswith('_id')]
    for col in id_cols:
        conn.execute(f'CREATE INDEX {table}_{col}_index on {table}({col})')


def parquet_to_sqlite(source, destination, db_name):
    tables = [name for name in os.listdir(source) if os.path.isdir(os.path.join(source, name))]
    conn = sqlite3.connect(os.path.join(destination, db_name)+'.sqlite')
    for table in sorted(tables):
        convert_table(source, table, conn)


def main():
    opts = parse_arguments()
    source = opts.source
    destination = opts.destination
    db_name = opts.name
    if not db_name:
        db_name = os.path.basename(source)
    parquet_to_sqlite(source, destination, db_name)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="Path to directory containing the parquet files")
    parser.add_argument("destination", help="Path to the location where you want to write the databse")
    parser.add_argument("--name", help="What to name the database.  Default is it uses the directory name pointed to")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s')
    main()
