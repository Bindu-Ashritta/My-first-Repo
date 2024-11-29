""" Delete All Files in an S3 Bucket
Goal: Write a script to delete all the files (objects) from a specific S3 bucket.
Steps:
1. Use boto3.resource('s3') to connect to the S3 bucket.
2. Loop through all objects in the bucket and delete them using delete() method.
3. Use logging to track deleted files. """

import boto3
import logging
from botocore.exceptions import *


# Set up logging to both console and a file
logging.basicConfig(
    level=logging.INFO,  # Set to INFO to capture info and higher-level logs
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Print logs to the console
        logging.FileHandler('s3_deletion.log')  # Log to a file
    ]
)

logging.basicConfig(filename='s3_deletion.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

bucket_name = 'mytestbucketboto'
s3 = boto3.client('s3')

def delete_all_objects(bucket_name):
    try:
        # List all objects in the S3 bucket
        objects = s3.list_objects_v2(Bucket=bucket_name)

        # Check if the bucket contains any objects
        if 'Contents' not in objects:
            logger.info(f"The bucket '{bucket_name}' is empty.")
            return

        # Prepare the objects to delete
        delete_objects = {'Objects': []}

        # Loop through the objects and add them to delete_objects list
        for obj in objects['Contents']:
            delete_objects['Objects'].append({'Key': obj['Key']})

            logger.info(f"Preparing to delete object: {obj['Key']}")

            # If the list has 1000 objects, delete them in a batch
            if len(delete_objects['Objects']) == 1000:
                response = s3.delete_objects(Bucket=bucket_name, Delete=delete_objects)
                logger.info(f"Deleted {len(delete_objects['Objects'])} objects.")
                delete_objects['Objects'] = []  # Reset for the next batch

        # Delete any remaining objects
        if delete_objects['Objects']:
            response = s3.delete_objects(Bucket=bucket_name, Delete=delete_objects)
            logger.info(f"Deleted {len(delete_objects['Objects'])} objects.")

    except ClientError as e:
        logger.error(f"Error deleting objects from '{bucket_name}': {e}")
        print(f"Error deleting objects: {e}")

# Call the function to delete all objects in the specified bucket
delete_all_objects(bucket_name)
