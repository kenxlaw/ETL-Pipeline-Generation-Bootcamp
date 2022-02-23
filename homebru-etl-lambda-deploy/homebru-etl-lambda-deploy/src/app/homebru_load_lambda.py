import logging
import boto3
import csv
import os
import pandas as pd
import app.database as database
import json
import os.path

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

    sqs_event = event["Records"][0]["body"]
    sqs_dict = json.loads(sqs_event)
    bucket_name = sqs_dict["bucket_name"]
    object_name = sqs_dict["bucket_key"]

    LOGGER.info(f"Triggered by file {object_name} in bucket {bucket_name}")

    s3 = boto3.client('s3')

    file_name = os.path.basename(object_name)
    file_path = f"/tmp/{file_name}"

    s3.download_file(bucket_name, object_name, file_path)

    # stripped_file = file_path.rsplit('_products.csv', 1)[0]
    # stripped_file = file_path.rsplit('_baskets.csv', 1)[0]
    # stripped_file = file_path.rsplit('_transactions.csv', 1)[0]

    creds = get_ssm_parameters_under_path("/team1/redshift")

    if "_products.csv" in object_name:
        products_transformed_data = read_products(file_path)        
        database.insert_products(creds, products_transformed_data)
        print(f"The products from {file_name} have successfully been loaded into the RedShift team1_cafe.products table")
    elif "_baskets.csv" in object_name:
        basket_transformed_data = read_basket(file_path)
        database.insert_basket(creds, basket_transformed_data)
        print(f"The baskets from {file_name} have successfully been loaded into the RedShift team1_cafe.basket table")
    elif "_transactions.csv" in object_name:
        transactions_transformed_data = read_transactions(file_path)
        database.insert_transactions(creds, transactions_transformed_data) 
        print(f"The orders from {file_name} have successfully been loaded into the RedShift team1_cafe.transactions table")
    else:
        print(f"Invalid file type for file {file_path}")

def get_data_type(object_name):
    if "_products.csv" in object_name:
        return "products"
    elif "_baskets.csv" in object_name:
        return "baskets"
    elif "_transactions.csv" in object_name:
        return "transactions"
    else:
        raise Exception("Unexpected data type")

def get_ssm_parameters_under_path(path: str) -> dict:

    ssm_client = boto3.client("ssm", region_name="eu-west-1")
    response = ssm_client.get_parameters_by_path(
        Path=path,
        Recursive=True,
        WithDecryption=True
    )
    formatted_response = {os.path.basename(x["Name"]):x["Value"] for x in response["Parameters"]}
    return formatted_response

def read_products(filename: str):
    with open(filename, 'r'):
        dict_reader = csv.DictReader(filename)
        ordered_dict_from_csv = list(dict_reader)[0]
        products_transformed_data = dict(ordered_dict_from_csv)
        print(products_transformed_data)
        return products_transformed_data

def read_basket(filename: str):
    with open(filename, 'r'):
        dict_reader = csv.DictReader(filename)
        ordered_dict_from_csv = list(dict_reader)[0]
        basket_transformed_data = dict(ordered_dict_from_csv)
        print(basket_transformed_data)
        return basket_transformed_data

def read_transactions(filename: str):
    with open(filename, 'r'):
        dict_reader = csv.DictReader(filename)
        ordered_dict_from_csv = list(dict_reader)[0]
        transactions_transformed_data = dict(ordered_dict_from_csv)
        print(transactions_transformed_data)
        return transactions_transformed_data