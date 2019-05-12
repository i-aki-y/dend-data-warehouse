import configparser
import psycopg2
from create_tables import get_hostname


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    hostname = get_hostname(config)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        hostname,
        config.get("CLUSTER","DB_NAME"),
        config.get("CLUSTER","DB_USER"),
        config.get("CLUSTER","DB_PASSWORD"),
        config.get("CLUSTER","DB_PORT"))
    )

    cur = conn.cursor()
    count_query = "select count(*) from {}"
    tables = ["staging_songs", "staging_events",
              "songplays", "users", "artists", "songs", "time"]

    for table in tables:
        cur.execute(count_query.format(table))
        for r in cur:
            print("{0}:{1}".format(table, r[0]))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
