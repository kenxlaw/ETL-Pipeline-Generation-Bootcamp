import logging
import boto3
import os
import pandas as pd
import app.extract_and_transform as extract_and_transform
import app.database as database
from sqlalchemy import create_engine

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

    s3_event = event["Records"][0]["s3"]
    bucket_name = s3_event["bucket"]["name"]
    object_name = s3_event["object"]["key"]

    file_path = (f"/tmp/{object_name}")

    LOGGER.info(f"Triggered by file {object_name} in bucket {bucket_name}")

    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_name, file_path)

    response = s3.list_objects(Bucket='homebru-cafe-data-bucket')
    print([x["Key"] for x in response["Contents"]])
    print(os.path.isfile('/tmp/chesterfield.csv'))
    
    # This part will be replaced with our ETL code to Transform our cafe data ready for RedShift
    products_data, order_products_data, orders_data = extract_and_transform.transform(f"/tmp/{object_name}")

    print(products_data)
    print(order_products_data)
    print(orders_data)
    
    creds = get_ssm_parameters_under_path("/team1/redshift")
    # database.create_table(creds)

    # This is needed for credentials to the RedShift database
    # conn = create_engine('postgresql://team1:nbdgAz4c9VTxrmGb@redshiftcluster-m1neadvazflf.ckpd1phjemrk.eu-west-1.redshift.amazonaws.com:5439/team1_cafe')

    # products_data.to_sql('products', conn, index=False, if_exists='append')
    # order_products_data.to_sql('basket', conn, index=False, if_exists='append')
    # orders_data.to_sql('transactions', conn, index=False, if_exists='append')

    # Try to get this working first and then try the rest
    database.persist_products(creds, products_data)
    # database.persist_order_products(creds, order_products_data)
    # database.persist_orders(creds, orders_data)
    

    # database.persist_order_products(creds, transformed_data["order_products_data"])
    # database.persist_orders(creds, transformed_data["orders_data"])

def get_ssm_parameters_under_path(path: str) -> dict:

    ssm_client = boto3.client("ssm", region_name="eu-west-1")
    response = ssm_client.get_parameters_by_path(
        Path=path,
        Recursive=True,
        WithDecryption=True
    )
    formatted_response = {os.path.basename(x["Name"]):x["Value"] for x in response["Parameters"]}
    return formatted_response