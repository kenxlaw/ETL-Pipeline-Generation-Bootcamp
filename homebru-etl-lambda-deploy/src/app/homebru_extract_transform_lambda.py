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

    file_name = os.path.basename(object_name)
    file_path = f"/tmp/{file_name}"

    create_backup(s3, bucket_name, object_name)

    s3.download_file(bucket_name, object_name, file_path)

    print(f"{file_name} has successfully been temporarily moved to the /tmp/ for Extract & Transform Lambda")

    results = extract_and_transform.transform(file_path)

    sqs = boto3.client('sqs')

    send_file(s3, sqs, results["products_data"], "products", object_name.rsplit('.', 1)[0] + "_products.csv")
    send_file(s3, sqs, results["order_products_data"], "baskets", object_name.rsplit('.', 1)[0] + "_baskets.csv")
    send_file(s3, sqs, results["orders_data"], "transactions", object_name.rsplit('.', 1)[0] + "_transactions.csv")
    
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
    queue_url = "https://sqs.eu-west-1.amazonaws.com/123980920791/homebru-cf-load-queue.fifo"
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json_message,
        MessageGroupId='1')
    LOGGER.info(f"Sending SQS message {json_message} to queue {queue_url}")


def write_csv(filename: str, data: list[dict[str, str]]):
    with open(filename, 'w', newline='') as csvfile:
        LOGGER.info(f"Python type: {type(data)}")
        LOGGER.info(f"File row 0 {data[0]}")
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def create_backup(s3, bucket_name, object_name):
    destination_bucket_name='homebru-cafe-data-backup-bucket'
    LOGGER.info(destination_bucket_name)
    #specify from where file needs to be copied
    copy_object={'Bucket':bucket_name,'Key':object_name}
    LOGGER.info(copy_object)
    #write copy statement 
    s3.copy_object(CopySource=copy_object,Bucket=destination_bucket_name,Key=object_name)
    LOGGER.info(f'A cafe data backup has been created.\n{object_name} has been been copied from {bucket_name} to {destination_bucket_name}')