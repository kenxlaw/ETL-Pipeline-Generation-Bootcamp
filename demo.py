import os  
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv
import uuid
from hashlib import sha256


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


#def extract_and_clean(filename):
#    try:
#        df = pd.read_csv(filename, names=[
#            'timestamp',  
#            'branch_name',
#            'customer_name',
#            'order_products',
#            'total_price',
#            'payment_type',
#            'card_number'])
#        df = df.drop(columns=['branch_name','customer_name','card_number'])
#        df = df.dropna()
#        print(df)
#    except Exception as error:
#        print("An error occurred: " + str(error))
#    return df
#extract_and_clean('chesterfield.csv')

def transform(filename):
    try:
        # from here onwards, until the next comment line is meant to be implemented from the extract.py file, but this execution/connection cannot be done yet
        df = pd.read_csv(filename, names=[
            'timestamp',  
            'branch_name',
            'customer_name',
            'order_products',
            'total_price',
            'payment_type',
            'card_number'])
        df = df.drop(columns=['branch_name','customer_name','card_number'])
        df = df.dropna()
        # end of previous comment
        # create uuid for each transaction
        df['order_id'] = [uuid.uuid4() for _ in range (len(df.index))]
        column_names = [
            'order_id',
            'timestamp', 
            'order_products',
            'total_price',
            'payment_type',]
        df = df.reindex(columns=column_names)
        # explode data to create orders table
        orders = pd.DataFrame(df.order_products.str.split(", ").tolist(), index = df.order_id).stack()
        orders = orders.reset_index([0,'order_id'])
        orders.columns = ['order_id','product']
        orders = pd.merge(orders, df[['timestamp','total_price','payment_type','order_id']], on='order_id', how='left')
        # add product_id
        orders['product_id'] = orders['product'].apply(lambda x: str(int(sha256(x.encode('utf-8')).hexdigest(), 16))[:10])
        # add product_price
        orders[['product_name', 'product_price']] = orders['product'].str.rsplit(' - ', 1, expand=True)
        orders = orders.drop(columns=['product'])
        # re-index orders table
        column_names = ['order_id',
                        'timestamp',
                        'product_id',
                        'product_name',
                        'product_price',
                        'total_price',
                        'payment_type']
        orders = orders.reindex(columns=column_names)
        # create order_products table
        order_products = orders.groupby(['order_id','product_id']).size()
        order_products.columns = ['order_id','product_id','quantity']
        # create product table
        products = orders[['product_id','product_name','product_price']]
        # clean orders table according to schema
        orders = orders.drop(columns=['product_id','product_name','product_price'])
    except Exception as error:
        print("An error occurred: " + str(error))
    return df

transform('chesterfield.csv')


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

def table_creation():
    sql = '''CREATE TABLE orders(
        uuid VARCHAR(255) NOT NULL,
        timestamp VARCHAR(255) NOT NULL,
        product_id VARCHAR(255) NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        product_price REAL NOT NULL,
        total_price REAL NOT NULL,
        payment_type VARCHAR(255) NOT NULL
        );'''
    execute_query(sql) 



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

extras.register_uuid()

def execute_many(filename, conn, table):
    #df = pd.read_csv(filename)
    #split_date_and_time = df["date_and_time"].str.split(" ", n = 1, expand = True)
    #df["date"]= split_date_and_time[0]
    #df["time"]= split_date_and_time[1]
    #df.drop(columns =["date_and_time"], inplace = True)
    #df['uuid'] = [uuid.uuid4() for _ in range (len(df.index))]
    #column_names = ['uuid',"date", "time", "branch_name", "order_products", "total_price", "payment_type"]
    #df = df.reindex(columns=column_names)

    df = pd.read_csv(filename, names=[
            'timestamp',  
            'branch_name',
            'customer_name',
            'order_products',
            'total_price',
            'payment_type',
            'card_number'])
    df = df.drop(columns=['branch_name','customer_name','card_number'])
    df = df.dropna()
    # end of previous comment
    # create uuid for each transaction
    df['uuid'] = [uuid.uuid4() for _ in range (len(df.index))]
    column_names = [
        'uuid',
        'timestamp', 
        'order_products',
        'total_price',
        'payment_type',]
    df = df.reindex(columns=column_names)
    # explode data to create orders table
    orders = pd.DataFrame(df.order_products.str.split(", ").tolist(), index = df.uuid).stack()
    orders = orders.reset_index([0,'uuid'])
    orders.columns = ['uuid','product']
    orders = pd.merge(orders, df[['timestamp','total_price','payment_type','uuid']], on='uuid', how='left')
    # add product_id
    orders['product_id'] = orders['product'].apply(lambda x: str(int(sha256(x.encode('utf-8')).hexdigest(), 16))[:10])
    # add product_price
    orders[['product_name', 'product_price']] = orders['product'].str.rsplit(' - ', 1, expand=True)
    orders = orders.drop(columns=['product'])
    # re-index orders table
    column_names = ['uuid',
                    'timestamp',
                    'product_id',
                    'product_name',
                    'product_price',
                    'total_price',
                    'payment_type']
    orders = orders.reindex(columns=column_names)
    # create order_products table
    order_products = orders.groupby(['uuid','product_id','product_name','product_price']).size()
    # create product table
    products = order_products.groupby(['product_id','product_name','product_price']).size()

    tuples = [tuple(x) for x in orders.to_numpy()]
    cols = ','.join(list(orders.columns))
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return df
    print("execute well done")
    cursor.close()


def main():
    #extract_and_clean('chesterfield.csv')
    transform('chesterfield.csv')
    conn = connect(**param_dict)
    table_creation() 
    execute_many('chesterfield.csv',conn,'orders')
    conn.close()

if __name__ == "__main__":
    main()