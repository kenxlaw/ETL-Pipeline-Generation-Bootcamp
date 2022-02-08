import uuid
import pandas as pd
from hashlib import sha256

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
        print(products)
    except Exception as error:
        print("An error occurred: " + str(error))
    return df

transform('chesterfield.csv')
