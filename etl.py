import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Runs each query specified in the copy_table_queries list that involve copying data from s3

    :parameter cur The cursor to execute the query on
    :parameter conn The connection to the database
    """

    num = 0
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        num+= 1
        print("Finished load query :", num)


def insert_tables(cur, conn):
    """
    Runs each query specified in the insert_table_queries list that involve inserting data from the staging tables to
    the analytics tables

    :parameter cur The cursor to execute the query on
    :parameter conn The connection to the database
    """

    num = 0
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        num += 1
        print("Finished insert query :", num)


def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()