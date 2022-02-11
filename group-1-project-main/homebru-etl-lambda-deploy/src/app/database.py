import sqlite3
import psycopg2
import os  
import boto3
import app.extract_and_transform as extract_and_transform

def execute_query(creds, queries: list[str]):

    with psycopg2.connect(f"dbname={creds['db']} user={creds['user']} password={creds['password']} host={creds['host']} port={creds['port']}"):

        with conn.cursor() as cur:
            for sql in queries:
                cur.execute(sql)
            conn.commit()

def persist_products(creds, products_data):
    
    sql = '''CREATE TABLE IF NOT EXISTS products (
             product_id VARCHAR(50) UNIQUE NOT NULL PRIMARY KEY,
             product_name VARCHAR(50),
             product_price FLOAT8           
            );'''

    # Start with products as its the easiest with the least values (check the original extract_and_transforms for help as you won't need to deploy anything)

    # Don't know if we need this extra part
    # products_data = ##(transformed data)

    # I think we should use the dictionary version of extract_and_transform to get and insert the correct values properly via Dict and not Dataframe


    # Load products into products table
    # sql =
    sql = '''INSERT INTO `products` (`product_id`, `product_name`, `product_price`) VALUES (%s, %s, %s)'''

    execute_query(creds, [sql])





# BELOW IS WHAT WILL BE USED FOR OTHER TABLES!
#------------------------------------------------

#
#     sql = '''CREATE TABLE IF NOT EXISTS transactions (
#              order_id VARCHAR(50) UNIQUE NOT NULL PRIMARY KEY, 
#              order_time timestamp,
#              total_price FLOAT8,
#              payment_method VARCHAR(50)          
#             );
           
#            CREATE TABLE IF NOT EXISTS basket (
#              order_id VARCHAR(50) UNIQUE NOT NULL,
#              product_id VARCHAR(50) UNIQUE NOT NULL, 
#              quantity INTEGER,
#              FOREIGN KEY(order_id) REFERENCES transactions(order_id),
#              FOREIGN KEY(product_id) REFERENCES products(product_id) 
#             );'''

#     execute_query(creds, [sql]) 