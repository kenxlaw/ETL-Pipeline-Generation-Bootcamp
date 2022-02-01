import os  #will use this in later revision
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras

param_dict = {
    "host"      : "localhost",
    "database"  : "dejabru",
    "user"      : "team1",
    "password"  : "password1"  ### will use os."""get from .env file for these parameters //data sensitivity WIP
}

#Extract

def extract_and_clean_sales_data():
    try:
        sales_data = pd.read_csv('chesterfield.csv', usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
        sales_data = sales_data.rename(columns={
        "date_and_time":"Date/Time",  
        "branch_name":"Branch",
        "order_products": "Basket",
        "total_price": "Total",
        "payment_type": "Payment"})
        print(sales_data)
    except Exception as error:
        print("An error occurred: " + str(error))
    return sales_data
extract_and_clean_sales_data()  #maybe add a df.drop in this func as well!?!?! currently runs w/o. Alt approach?!?

def connect(param_dict):
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
        df = df.rename(columns={
        "date_and_time":"Date/Time",  
        "branch_name":"Branch",
        "order_products": "Basket",
        "total_price": "Total",
        "payment_type": "Payment"})

read_dataframe()

def execute_query(sql):
    conn = None
    try:
        conn = psycopg2.connect(**param_dict)   # We got Kwargs!!! It uses the dict above
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

            date_and_time VARCHAR(255) NOT NULL,
            branch_name VARCHAR(255) NOT NULL,
            order_products VARCHAR(500) NOT NULL,
            total_price VARCHAR(255) NOT NULL,
            payment_type VARCHAR(255) NOT NULL
            );

            CREATE TABLE branches(
            branch_id SERIAL NOT NULL PRIMARY KEY,
            branch_name VARCHAR(255) NOT NULL
            );

            CREATE TABLE customers(
            customer_id SERIAL NOT NULL PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            branch_id INT NOT NULL
            );

            CREATE TABLE products(
            product_id SERIAL NOT NULL PRIMARY KEY,
            product_name VARCHAR(500) NOT NULL,
            product_price VARCHAR(255) NOT NULL
            );

            CREATE TABLE transactions(
            transactions_id INT NOT NULL PRIMARY KEY,
            branch_id INT NOT NULL,
            date DATE NOT NULL,
            time TIMESTAMP NOT NULL,
            customer_name VARCHAR(255) NOT NULL,
            payment_type VARCHAR(255) NOT NULL,
            total_price VARCHAR(255) NOT NULL
            );
        
            CREATE TABLE basket(
            order_id SERIAL NOT NULL PRIMARY KEY,
            product_id INT NOT NULL REFERENCES products(product_id),
            quantity INT NOT NULL
            );'''


    execute_query(sql) #Test parameters so far, ALTER table or change function code once we want it cleaner.
    ### changed it to (500) because something went past the 255 on order_products. (wouldn't let me complete a load)

chesterfield_table_creation()  #hash it out or drop table when testing, as you already will have this table made after you use it once.

#### may need to make a if table exist == true then pass kinda thing here for future implement. If table exist != create table etc.

def single_load():
    sql = '''INSERT INTO "chesterfield" ("date_and_time", "branch_name", order_products, "total_price", "payment_type") 
            VALUES ('25/08/2021 09:00', 'Chesterfield', 'Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45', '5.2', 'CARD');'''
    execute_query(sql)

#single_load() #test run example works. 

def execute_many(conn, df, table):
    df = pd.read_csv('chesterfield.csv', usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
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
    conn = connect(param_dict)
    execute_many(conn,df,'chesterfield')
    conn.close()

if __name__ == "__main__":
    main()
