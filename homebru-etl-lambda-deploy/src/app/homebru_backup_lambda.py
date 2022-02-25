import logging
import boto3
import json

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
client = boto3.client('s3')

def lambda_handler(event, context):
    LOGGER.info(event)

    #specify source bucket
    source_bucket_name=event['Records'][0]['s3']['bucket']['name']
    LOGGER.info(source_bucket_name)
    #get object that has been uploaded
    file_name=event['Records'][0]['s3']['object']['key']
    LOGGER.info(file_name)
    #specify destination bucket
    destination_bucket_name='homebru-cafe-data-backup-bucket'
    LOGGER.info(destination_bucket_name)
    #specify from where file needs to be copied
    copy_object={'Bucket':source_bucket_name,'Key':file_name}
    LOGGER.info(copy_object)
    #write copy statement 
    client.copy_object(CopySource=copy_object,Bucket=destination_bucket_name,Key=file_name)
    LOGGER.info(client.copy_object(CopySource=copy_object,Bucket=destination_bucket_name,Key=file_name))

    return {
        'statusCode': 3000,
        'body': json.dumps(f'A cafe data backup has been created.\n{file_name} has been been copied from {source_bucket_name} to {destination_bucket_name}')
    }

    s3_event = event["Records"][0]["s3"]
    bucket_name = s3_event["bucket"]["name"]
    object_name = s3_event["object"]["key"]

    LOGGER.info(f"Triggered by file {object_name} in bucket {bucket_name}")

    s3 = boto3.client('s3')

    file_name = os.path.basename(object_name)

    s3.download_file(bucket_name, object_name, object_name)

    print(f"{file_name} has successfully been downloaded to the Homebru Cafe Data Backup Bucket in the location {object_name}")