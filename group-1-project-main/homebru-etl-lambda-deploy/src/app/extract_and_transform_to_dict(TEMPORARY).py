import uuid
import pandas as pd
from hashlib import sha256

def transform(filename):
    try:
        # from here onwards, until the next comment line is meant to be implemented from the extract.py file, but this execution/connection cannot be done yet
        df = pd.read_csv(filename, names=[
            'order_time',  
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
            'order_time', 
            'order_products',
            'total_price',
            'payment_type']
        df = df.reindex(columns=column_names)
        # explode data to create orders table
        orders = pd.DataFrame(df.order_products.str.split(", ").tolist(), index = df.order_id).stack()
        orders = orders.reset_index([0,'order_id'])
        orders.columns = ['order_id','product']
        orders = pd.merge(orders, df[['order_time','total_price','payment_type','order_id']], on='order_id', how='left')
        # add product_id
        orders['product_id'] = orders['product'].apply(lambda x: str(int(sha256(x.encode('utf-8')).hexdigest(), 16))[:10])
        # add product_name and product_price
        orders[['product_name', 'product_price']] = orders['product'].str.rsplit(' - ', 1, expand=True)
        
        # do these parts before you remove stuff from orders
        order_products = orders.groupby(['order_id','product_id','product_name','product_price'])

        # remove unnecessary columns and add quantity after calculating the duplicates
        order_products = order_products.size().reset_index(name='quantity')
        column_names = ['order_id',
                        'product_id',
                        'quantity']
        order_products_data = order_products.reindex(columns=column_names)
        orders_products_data = orders_products_data.to_dict()

                
        # sort the orders table to be converted for the products table
        products = orders.groupby(['product_id','product_name','product_price'])
        products = products.size().reset_index()
        column_names = ['product_id',
                        'product_name',
                        'product_price']
        products_data = products.reindex(columns=column_names)
        products_data = products_data.to_dict()

        orders = orders.drop(columns=['product','product_id','product_name','product_price'])
        # re-index orders table
        column_names = ['order_id',
                        'order_time',
                        'payment_type',
                        'total_price']
        orders = orders.reindex(columns=column_names)
        orders_data = orders.drop_duplicates()
        orders_data = orders_data.to_dict()
            
    except Exception as error:
        print("An error occurred: " + str(error))
        
    print(orders_data, order_products_data, products_data)
    return orders_data, order_products_data, products_data
