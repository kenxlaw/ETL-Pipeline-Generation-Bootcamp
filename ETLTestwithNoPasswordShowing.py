import os  #will use this in later revision
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

def extract_and_clean_sales_data():
    try:
        df = pd.read_csv('chesterfield.csv', usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
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
extract_and_clean_sales_data()

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

def read_dataframe():
        df = pd.read_csv('chesterfield.csv', usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
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

read_dataframe()

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

    execute_query(sql) #Test parameters so far, ALTER table or change function code once we want it cleaner.
    ### changed it to (500) because something went past the 255 on order_products. (wouldn't let me complete a load)

chesterfield_table_creation() 

#### may need to make a if table exist == true then pass kinda thing here for future implement. If table exist != create table etc.
extras.register_uuid()

def single_load():
    sql = '''INSERT INTO "chesterfield" ("uuid", "date", "time", "branch_name", order_products, "total_price", "payment_type") 
            VALUES ('1234','25/08/2021', '09:00', 'Chesterfield', 'Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45', '5.2', 'CARD');'''
    execute_query(sql)

#single_load() #test run example works. 

def execute_many(conn, df, table):
    df = pd.read_csv('chesterfield.csv', usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
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
    print("execute_values() done")
    cursor.close()


###below acts as the run all function.

def main():
    df = read_dataframe()
    conn = connect(**param_dict)
    execute_many(conn,df,'chesterfield')
    conn.close()

if __name__ == "__main__":
    main()
