import psycopg2
import psycopg2.extras as extras
import os  
import pandas as pd
import uuid
from src import extract_and_transform

host = os.environ.get("POSTGRES_HOST")
user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
db_name = os.environ.get("POSTGRES_DB")
conn = psycopg2.connect(
host=host,
user=user,
password=password,
database=db_name
)

param_dict = {
    "host"      : host,
    "database"  : db_name,
    "user"      : user,
    "password"  : password
}

def execute_query(sql):
    conn = None
    try:
        conn = psycopg2.connect(**param_dict)   
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def chesterfield_table_creation():
    sql = '''CREATE TABLE chesterfield(
            uuid VARCHAR(255) NOT NULL,
            date VARCHAR(255) NOT NULL,
            time VARCHAR(255) NOT NULL,
            branch_name VARCHAR(255) NOT NULL,
            order_products VARCHAR(500) NOT NULL,
            total_price REAL NOT NULL,
            payment_type VARCHAR(255) NOT NULL
            ); '''

    execute_query(sql) 

chesterfield_table_creation() 

extras.register_uuid()

def execute_many(filename, conn, df, table):
    
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 
    print("execute well done")
    cursor.close()


###below acts as the run all function.

def main():
    df = read_dataframe('chesterfield.csv')
    conn = connect(**param_dict)
    execute_many('chesterfield.csv',conn,df,'chesterfield')
    conn.close()

# if __name__ == "__main__":
