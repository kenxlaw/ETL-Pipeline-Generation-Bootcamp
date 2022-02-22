import logging
from operator import contains
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

    s3_event = event["Records"][0]["s3"]
    bucket_name = s3_event["bucket"]["name"]
    object_name = s3_event["object"]["key"]

    LOGGER.info(f"Triggered by file {object_name} in bucket {bucket_name}")

    s3 = boto3.client('s3')

    file_name = os.path.basename(object_name)
    file_path = f"/tmp/{file_name}"

    s3.download_file(bucket_name, object_name, file_path)

    stripped_file = file_path.rsplit('_products', 1)[0]
    stripped_file = file_path.rsplit('_basket', 1)[0]
    stripped_file = file_path.rsplit('_transactions', 1)[0]

    creds = get_ssm_parameters_under_path("/team1/redshift")

    if os.path.is_file(stripped_file + "_products"):
        products_transformed_data = read_products(file_path)        
        database.insert_products(creds, products_transformed_data)
        print(f"The products from {file_name} have successfully been loaded into the RedShift team1_cafe.products table")
    elif os.path.is_file(stripped_file + "_basket"):
        basket_transformed_data = read_basket(file_path)
        database.insert_basket(creds, basket_transformed_data)
        print(f"The baskets from {file_name} have successfully been loaded into the RedShift team1_cafe.basket table")
    elif os.path.is_file(stripped_file + "_transactions"):
        transactions_transformed_data = read_transactions(file_path)
        database.insert_transactions(creds, transactions_transformed_data) 
        print(f"The orders from {file_name} have successfully been loaded into the RedShift team1_cafe.transactions table")
    else:
        print(f"Invalid file type for file {file_path}")

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