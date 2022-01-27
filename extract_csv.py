import csv
import pandas as pd 

def extract_and_clean_sales_data():
    try:
        sales_data = pd.read_csv('chesterfield.csv', usecols=['date_and_time', 'branch_name', 'order_products', 'total_price', 'payment_type'])
        print(sales_data)
    except Exception as error:
        print("An error occurred: " + str(error))
    return sales_data

extract_and_clean_sales_data()