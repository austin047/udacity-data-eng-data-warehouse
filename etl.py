import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: Load data from the s3 butcket into the redshift database.
    
    Arguments: 
        cur: cursor Object
        conn: database connection
    
    Returns: 
        None
    """
    
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit() 
        except Exception as e:
            print(e)



def insert_tables(cur, conn):
    """
    Description: Insert date into the dimension and facts table (using data from staging tables).
    
    Arguments: 
        cur: cursor Object
        conn: database connection
    
    Returns: 
        None
    """
        
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit() 
        except Exception as e:
            print(e)


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