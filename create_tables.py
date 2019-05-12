import os
import configparser
import boto3
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def get_hostname(config):
    """Get hostname of redshift by using cluster identifier"""
    KEY = os.environ.get("DWH_AWS_KEY")
    SECRET = os.environ.get("DWH_AWS_SECRET")
    redshift = boto3.client('redshift', region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    CLUSTER_IDENTIFIER = config.get("CLUSTER", "CLUSTER_IDENTIFIER")
    cluster_props = redshift.describe_clusters(
        ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    endpoint = cluster_props["Endpoint"]["Address"]
    return endpoint


def drop_tables(cur, conn):
    """Drop tables created by create_tables method if they exist"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create stable and target tables if they don't exists"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Creat tables in the redshift cluster"""
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
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
