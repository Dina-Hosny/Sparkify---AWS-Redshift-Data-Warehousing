import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Load the JSON files from S3 buckets into the staging tables
    
    INPUTS: 
    * cur the cursor variable
    * conn the database connection
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    Load data from the staging tables into existing tables
    that defined in the insert_table_queries list.
    
    INPUTS: 
    * cur the cursor variable
    * conn the database connection
    
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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