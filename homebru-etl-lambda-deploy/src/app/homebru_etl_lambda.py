import logging
import boto3
import os
import pandas as pd
import app.extract_and_transform as extract_and_transform
import app.database as database

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
        
    creds = get_ssm_parameters_under_path("/team1/redshift")

    results = extract_and_transform.transform(f"/tmp/{object_name}")

    product_data = results["products_data"]
    # print(product_data)
    database.insert_products(creds, product_data)

    order_products_data = results["order_products_data"]
    # # print(order_products_data)
    database.insert_basket(creds, order_products_data)

    orders_data = results["orders_data"]
    # print(orders_data)
    database.insert_transactions(creds, orders_data)  

def get_ssm_parameters_under_path(path: str) -> dict:

    ssm_client = boto3.client("ssm", region_name="eu-west-1")
    response = ssm_client.get_parameters_by_path(
        Path=path,
        Recursive=True,
        WithDecryption=True
    )
    formatted_response = {os.path.basename(x["Name"]):x["Value"] for x in response["Parameters"]}
    return formatted_response