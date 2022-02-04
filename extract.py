import pandas as pd

def extract_and_clean(filename):
    try:
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
        print(df)
    except Exception as error:
        print("An error occurred: " + str(error))
    return df

extract_and_clean('chesterfield.csv') 