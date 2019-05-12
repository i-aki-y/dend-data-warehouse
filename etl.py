import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_tables import get_hostname

def load_staging_tables(cur, conn):
    """Load data from S3 to staging tables by using copy statement"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transform data from staging table to target tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Load data from S3 to target tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    hostname = get_hostname(config)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        hostname,
        config.get("CLUSTER", "DB_NAME"),
        config.get("CLUSTER", "DB_USER"),
        config.get("CLUSTER", "DB_PASSWORD"),
        config.get("CLUSTER", "DB_PORT"))
    )

    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
