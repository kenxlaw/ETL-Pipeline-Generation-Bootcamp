import logging
import boto3
import os
import pandas as pd

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
    extract_and_clean(f"/tmp/{object_name}")

    # This last part will be the final load into RedShift