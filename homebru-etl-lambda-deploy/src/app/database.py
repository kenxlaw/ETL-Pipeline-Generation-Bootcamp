import sqlite3
import logging
import psycopg2
import os  
import boto3
import psycopg2.extras as extras
import pandas as pd

# LOGGER = logging.getLogger()
# LOGGER.setLevel(logging.INFO)

# def execute_query(creds, queries: list[str]):

#     # LOGGER.info(creds)
#     with psycopg2.connect(f"dbname={creds['db']} user={creds['user']} password={creds['password']} host={creds['host']} port={creds['port']}") as conn:

#         with conn.cursor() as cur:
#             for sql in queries:
#                 # LOGGER.info(f"Executing {sql}")
#                 cur.execute(sql)
#             conn.commit()

def persist_products(creds, products_data):
    conn = psycopg2.connect(f"dbname={creds['db']} user={creds['user']} password={creds['password']} host={creds['host']} port={creds['port']}")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO products(product_id,product_name,product_price) SELECT product_id,product_name,product_price FROM " + {products_data} + ");")
        pd.DataFrame = cur.fetch_dataframe()
    conn.commit()


    # sql_texts = []
    # for index, row in products_data.iterrows():       
    #     sql_texts.append('INSERT INTO ' + creds + ' (' + str(', '.join(products_data.columns)) + ') VALUES ' + str(tuple(row.values)))        
    
    # sql = "INSERT INTO products(product_id,product_name,product_price) SELECT product_id,product_name,product_price FROM " + f"{products_data}" + ");"
    # sql = f"INSERT INTO products (SELECT * from :{products_data});"

    # execute_query(creds, [sql])

# def create_table(creds):

#     sql= '''CREATE TABLE IF NOT EXISTS products (
#             product_id VARCHAR(50) UNIQUE NOT NULL PRIMARY KEY,
#             product_name VARCHAR(50),
#             product_price FLOAT8
#             );
            
#             CREATE TABLE IF NOT EXISTS order_products (
#             order_id VARCHAR(50) UNIQUE NOT NULL,            
#             product_id VARCHAR(50) UNIQUE NOT NULL, 
#             quantity INTEGER,
#             FOREIGN KEY(order_id) REFERENCES orders(order_id),
#             FOREIGN KEY(product_id) REFERENCES products(product_id) 
#             );

#             CREATE TABLE IF NOT EXISTS orders (
#             order_id VARCHAR(50) UNIQUE NOT NULL PRIMARY KEY, 
#             order_time timestamp,            
#             total_price FLOAT8,
#             payment_method VARCHAR(50)          
#             );'''

#     execute_query(creds, [sql])

# def persist_products(creds, products_data):

    

#     # sql_texts = []
#     # for index, row in products_data.iterrows():       
#     #     sql_texts.append('INSERT INTO ' + creds + ' (' + str(', '.join(products_data.columns)) + ') VALUES ' + str(tuple(row.values)))        
    
#     # sql = "INSERT INTO products(product_id,product_name,product_price) SELECT product_id,product_name,product_price FROM " + f"{products_data}" + ");"
#     sql = f"INSERT INTO products (SELECT * from :{products_data});"

#     execute_query(creds, [sql])

# def persist_order_products(creds, order_products_data):
    
#     sql = f'''INSERT INTO basket 
#               SELECT * from {order_products_data}
#               );'''

#     execute_query(creds, [sql])

# def persist_orders(creds, orders_data):
    
#     sql = f'''INSERT INTO transactions 
#               SELECT * from {orders_data}
#               );'''

#     execute_query(creds, [sql])


# def persist_transactions(creds, products_data):
    
#     sql = '''CREATE TABLE IF NOT EXISTS products (
#              product_id VARCHAR(50) UNIQUE NOT NULL PRIMARY KEY,
#              product_name VARCHAR(50),
#              product_price FLOAT8           
#             );'''

    # Start with products as its the easiest with the least values (check the original extract_and_transforms for help as you won't need to deploy anything)

    # Don't know if we need this extra part
    # products_data = ##(transformed data)

    # I think we should use the dictionary version of extract_and_transform to get and insert the correct values properly via Dict and not Dataframe


    # Load products into products table
    # sql =
    # sql = '''INSERT INTO `products` (`product_id`, `product_name`, `product_price`) VALUES (%s, %s, %s)'''





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