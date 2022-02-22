import pandas as pd
from hashlib import sha256, md5
import csv

def transform(filename):
    try:
        # from here onwards, until the next comment line is meant to be implemented from the extract.py file, but this execution/connection cannot be done yet
        df = pd.read_csv(filename, names=[
            'order_time',  
            'branch_name',
            'customer_name',
            'order_products',
            'total_price',
            'payment_method',
            'card_number'])
        df = df.drop(columns=['customer_name','card_number'])
        df = df.dropna()
        # end of previous comment
        # create order id for each transaction
        df['order_id'] =  df.apply(lambda x:md5((str(x[0])+str(x[1])).encode('utf8')).hexdigest(), axis=1)
        df['order_time'] = pd.to_datetime(df['order_time'])
        column_names = [
            'order_id',
            'order_time',
            'branch_name', 
            'order_products',
            'total_price',
            'payment_method']
        df = df.reindex(columns=column_names)
        # explode data to create orders table
        orders = pd.DataFrame(df.order_products.str.split(", ").tolist(), index = df.order_id).stack()
        orders = orders.reset_index([0,'order_id'])
        orders.columns = ['order_id','product']
        orders = pd.merge(orders, df[['branch_name','order_time','total_price','payment_method','order_id']], on='order_id', how='left')
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

                
        # sort the orders table to be converted for the products table
        products = orders.groupby(['product_id','product_name','product_price'])
        products = products.size().reset_index()
        column_names = ['product_id',
                        'product_name',
                        'product_price']
        products_data = products.reindex(columns=column_names)

        orders = orders.drop(columns=['product','product_id','product_name','product_price'])
        # re-index orders table
        column_names = ['branch_name',
                        'order_id',
                        'order_time',
                        'total_price',
                        'payment_method']
        orders = orders.reindex(columns=column_names)
        orders_data = orders.drop_duplicates()
        
        results = {
            "products_data" : products_data.T.to_dict().values(),
            "order_products_data" : order_products_data.T.to_dict().values(),
            "orders_data" : orders_data.T.to_dict().values()
        }
        products_data = results["products_data"]
        order_products_data = results["order_products_data"]
        orders_data = results["orders_data"]
        
        basket_fieldnames = ['order_id','product_id','quantity']
        products_fieldnames = ['product_id','product_name','product_price']
        transactions_fieldnames = ['order_id','branch_name','order_time','total_price','payment_method']
        
        # write basket.csv
        with open("basket.csv", "w") as order_products_csvfile:
            writer = csv.DictWriter(order_products_csvfile, fieldnames=basket_fieldnames)
            writer.writeheader()
            writer.writerows(order_products_data)
        # write products.csv
        with open("products.csv", "w") as products_csvfile:
            writer = csv.DictWriter(products_csvfile, fieldnames=products_fieldnames)
            writer.writeheader()
            writer.writerows(products_data)
        # write transactions.csv
        with open("transactions.csv", "w") as orders_csvfile:
            writer = csv.DictWriter(orders_csvfile, fieldnames=transactions_fieldnames)
            writer.writeheader()
            writer.writerows(orders_data)
        
        return results
    
    except Exception as error:
        print("An error occurred: " + str(error))