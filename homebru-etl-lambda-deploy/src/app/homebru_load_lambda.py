import logging
import os
import pandas as pd
import app.extract_and_transform as extract_and_transform
import app.database as database

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

#     creds = get_ssm_parameters_under_path("/team1/redshift")

#     product_data = results["products_data"]
#     # print(product_data)
#     database.insert_products(creds, product_data)
#     print(f"The products from {file_name} have successfully been loaded into the RedShift team1_cafe.products table")

#     order_products_data = results["order_products_data"]
#     # print(order_products_data)
#     database.insert_basket(creds, order_products_data)
#     print(f"The baskets from {file_name} have successfully been loaded into the RedShift team1_cafe.basket table")

#     orders_data = results["orders_data"]
#     # print(orders_data)
#     database.insert_transactions(creds, orders_data) 
#     print(f"The orders from {file_name} have successfully been loaded into the RedShift team1_cafe.transactions table")
#     print(f"All data from {file_name} have been successfully extracted, transformed and loaded into RedShift team1_cafe tables")

# def get_ssm_parameters_under_path(path: str) -> dict:

#     ssm_client = boto3.client("ssm", region_name="eu-west-1")
#     response = ssm_client.get_parameters_by_path(
#         Path=path,
#         Recursive=True,
#         WithDecryption=True
#     )
#     formatted_response = {os.path.basename(x["Name"]):x["Value"] for x in response["Parameters"]}
#     return formatted_response




    
