import logging
import boto3
import csv
import os
import pandas as pd
import app.extract_and_transform as extract_and_transform
import json

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def lambda_handler(event, context):
    LOGGER.info(event)

    s3_event = event["Records"][0]["s3"]
    bucket_name = s3_event["bucket"]["name"]
    object_name = s3_event["object"]["key"]

    LOGGER.info(f"Triggered by file {object_name} in bucket {bucket_name}")

    s3 = boto3.client('s3')

    # os.path.basename might be causing error with HeadObject in cloud logs, otherwise looks like it works fine, idk

    file_name = os.path.basename(object_name)
    file_path = f"/tmp/{file_name}"

    s3.download_file(bucket_name, object_name, file_path)

    print(f"{file_name} has successfully been temporarily moved to the /tmp/ for Extract & Transform Lambda")

    results = extract_and_transform.transform(file_path)

    sqs = boto3.client('sqs')

    send_file(s3, sqs, results["products_data"], "products", file_name.rsplit('.', 1)[0] + "_products.csv")
    LOGGER.info(event)
    send_file(s3, sqs, results["order_products_data"], "order_products", file_name.rsplit('.', 1)[0] + "_baskets.csv")
    LOGGER.info(event)
    send_file(s3, sqs, results["orders_data"], "orders", file_name.rsplit('.', 1)[0] + "_transactions.csv")
    LOGGER.info(event)


def send_file(s3, sqs, data_set, data_type: str, bucket_key: str):

    LOGGER.info(f"Sending transformed data set of type {data_type} with {len(data_set)} rows")
    write_csv("/tmp/output.csv", data_set)
    LOGGER.info(f"Wrote a local CSV for: {data_set}")

    bucket_name = "homebru-cafe-transformed-data-bucket"
    s3.upload_file("/tmp/output.csv", bucket_name, bucket_key)
    LOGGER.info(f"Uploading to S3 into bucket {bucket_name} with key {bucket_key}")

    message = {
        "bucket_name" : bucket_name,
        "bucket_key" : bucket_key,
        "data_type" : data_type
    }
    
    json_message = json.dumps(message)
    queue_url = "https://sqs.eu-west-1.amazonaws.com/123980920791/homebru-cf-load-queue"
    LOGGER.info(f"Sending SQS message {json_message} to queue {queue_url}")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json_message)

def write_csv(filename: str, data: list[dict[str, str]]):
    with open(filename, 'w', newline='') as csvfile:
        LOGGER.info(f"Python type: {type(data)}")
        LOGGER.info(f"File row 0 {data[0]}")
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


        # basket_fieldnames = ['order_id','product_id','quantity']
        # products_fieldnames = ['product_id','product_name','product_price']
        # transactions_fieldnames = ['order_id','branch_name','order_time','total_price','payment_method']
        
        # # write basket.csv
        # with open("basket.csv", "w") as order_products_csvfile:
        #     writer = csv.DictWriter(order_products_csvfile, fieldnames=basket_fieldnames)
        #     writer.writeheader()
        #     writer.writerows(order_products_data)
        # # write products.csv
        # with open("products.csv", "w") as products_csvfile:
        #     writer = csv.DictWriter(products_csvfile, fieldnames=products_fieldnames)
        #     writer.writeheader()
        #     writer.writerows(products_data)
        # # write transactions.csv
        # with open("transactions.csv", "w") as orders_csvfile:
        #     writer = csv.DictWriter(orders_csvfile, fieldnames=transactions_fieldnames)
        #     writer.writeheader()
        #     writer.writerows(orders_data)


    
    # basket_fieldnames = ['order_id','product_id','quantity']
    # products_fieldnames = ['product_id','product_name','product_price']
    # transactions_fieldnames = ['branch_name','order_id','order_time','total_price','payment_method']
        
    # response = s3.list_objects(Bucket='homebru-cafe-data-bucket')
    # print([x["Key"] for x in response["Contents"]])