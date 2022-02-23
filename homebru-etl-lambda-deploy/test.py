import boto3
import logging
import json

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def sqs_function(bucket_name, bucket_key, data_type):
    
    sqs = boto3.resource('sqs')
    print("here")
     # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName='homebru-cf-load-queue')
    print("queue")

    # You can now access identifiers and attributes
    LOGGER.info(queue.url)
    LOGGER.info(queue.attributes.get('DelaySeconds'))

    message = {
        "bucket_name" : bucket_name,
        "bucket_key" : bucket_key,
        "data_type" : data_type
    }
    
    json_message = json.dumps(message)   
    response = queue.send_message(MessageBody=json_message)
    print("response")

    LOGGER.info(response.get('MessageId'))
    LOGGER.info(response.get('MD5OfMessageBody'))

sqs_function("Test", "Test key", "Products")