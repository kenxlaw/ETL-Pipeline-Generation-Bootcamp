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