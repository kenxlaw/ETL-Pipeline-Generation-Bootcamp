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
    
