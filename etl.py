import psycopg2

from configparser import ConfigParser, ExtendedInterpolation
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read('dwh.cfg')

    db_host = parser.get("cluster", "db_host")
    db_name = parser.get("cluster", "db_name")
    db_user = parser.get("cluster", "db_user")
    db_password = parser.get("cluster", "db_password")
    db_port = parser.get("cluster", "db_port")
    db_info = f"host={db_host} dbname={db_name} user={db_user} password={db_password} port={db_port}"
    conn = psycopg2.connect(db_info)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()