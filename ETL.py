import os  
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv
import uuid


load_dotenv()
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
#Extract



def extract_and_clean_sales_data(filename):
    try:
        df = pd.read_csv(filename, usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
        split_date_and_time = df["date_and_time"].str.split(" ", n = 1, expand = True)
        df["date"]= split_date_and_time[0]
        df["time"]= split_date_and_time[1]
        df.drop(columns =["date_and_time"], inplace = True)
        df['uuid'] = [uuid.uuid4() for _ in range (len(df.index))]
        column_names = ['uuid',"date", "time", "branch_name", "order_products", "total_price", "payment_type"]
        df = df.reindex(columns=column_names)
        sales_data = df.rename(columns={
        "uuid":"UUID",
        "date":"Date",
        "time":"Time",  
        "branch_name":"Branch",
        "order_products": "Basket",
        "total_price": "Total",
        "payment_type": "Payment"})
        print(sales_data)
    except Exception as error:
        print("An error occurred: " + str(error))
    return df
extract_and_clean_sales_data('chesterfield.csv')

def connect(**param_dict):
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**param_dict)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print('error')
    print("Connection successful")
    return conn

def read_dataframe(filename):
        df = pd.read_csv(filename, usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
        split_date_and_time = df["date_and_time"].str.split(" ", n = 1, expand = True)
        df["date"]= split_date_and_time[0]
        df["time"]= split_date_and_time[1]
        df.drop(columns =["date_and_time"], inplace = True)
        df['uuid'] = [uuid.uuid4() for _ in range (len(df.index))]
        column_names = ['uuid',"date", "time", "branch_name", "order_products", "total_price", "payment_type"]
        df = df.reindex(columns=column_names)
        df = df.rename(columns={
        "uuid":"UUID",
        "date":"Date",
        "time":"Time",  
        "branch_name":"Branch",
        "order_products": "Basket",
        "total_price": "Total",
        "payment_type": "Payment"})

read_dataframe('chesterfield.csv')

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
    df = pd.read_csv(filename, usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
    split_date_and_time = df["date_and_time"].str.split(" ", n = 1, expand = True)
    df["date"]= split_date_and_time[0]
    df["time"]= split_date_and_time[1]
    df.drop(columns =["date_and_time"], inplace = True)
    df['uuid'] = [uuid.uuid4() for _ in range (len(df.index))]
    column_names = ['uuid',"date", "time", "branch_name", "order_products", "total_price", "payment_type"]
    df = df.reindex(columns=column_names)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
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

if __name__ == "__main__":
    main()

