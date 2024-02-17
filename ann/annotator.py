# annotator.py
#
# NOTE: This file lives on the AnnTools instance
#
# Copyright (C) 2013-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import botocore
import json
import os
import sys
import time
from subprocess import Popen, PIPE
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

base_dir = os.path.abspath(os.path.dirname(__file__))

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read(os.path.join(base_dir, "annotator_config.ini"))


"""Reads request messages from SQS and runs AnnTools as a subprocess.

Move existing annotator code here
"""


def handle_requests_queue(sqs=None):

    # Read messages from the queue
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])

    s3_rsc = boto3.resource('s3', region_name = config['aws']['AwsRegionName'],config = botocore.client.Config(signature_version=config['aws']['SignatureVersion']))


    # Receive message from SQS 
    #ref doc: https://aws.amazon.com/cn/sqs/getting-started/
    #ref doc: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html
    #ref doc: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/step-receive-delete-message.html
    # Attempt to read the maximum number of messages from the queue
    try:
        messages = sqs.receive_message(
            QueueUrl = config['sqs']['RequestQueueUrl'],
            MaxNumberOfMessages = int(config['sqs']['MaxMessages']),
            WaitTimeSeconds =int(config['sqs']['WaitTime'])) 
    except ClientError as e:
        print(f"Failed to receive messages: {e.response['Error']['Message']}")
    

    # Process messages received
    #ref doc: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/confirm-queue-is-empty.html
    # Use long polling - DO NOT use sleep() to wait between polls
    if 'Messages' in messages:
        for message in messages['Messages']:
            try:
                # load the message of json format
                sns_data = json.loads(message['Body'])
                job_data = json.loads(sns_data['Message'])
            except KeyError:
                return

            # extract job parameters from the message body as before
            job_id = job_data["job_id"] #uuid
            user_id = job_data["user_id"] #userX
            file_name = job_data["input_file_name"] #filename
            bucket_name = job_data["s3_input_bucket"]
            s3_key = job_data["s3_key_input_file"] #CNetID/userX/uuid~filename
            job_name = job_id + '~' + file_name

            if not job_id or not bucket_name or not user_id or not file_name or not s3_key:
                print (f"Missing required data in request")
                return 

            

            # create folder for all currrent user's jobs in EC, e.g. /home/ubuntu/gas/ann/userX
            jobs_dir = os.path.join(base_dir,user_id)

            try:
                os.makedirs(jobs_dir, exist_ok=True)
            except OSError as e:
                # ref of handling system error: https://www.geeksforgeeks.org/handling-oserror-exception-in-python/
                print(f"Make jobs folder failed: {e}")
                return

            # Get the input file S3 object and copy it to a local file
            # ref of download data from s3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
            try:
                s3_rsc.meta.client.download_file(bucket_name, s3_key, '{}'.format(job_name))
            except ClientError as e:
                print(f"Failed to download input file from S3: {e}")
                return
                
            if os.path.exists('./{}'.format(job_name)):  
                try:
                    os.system('mv ./{} {}'.format(job_name, jobs_dir)) 
                except OSError as e:
                    print(f"Move file failed:{e}")
                    return
            else:
                print("Move file failed. File does not exists")
                return

            local_file_abs_dir = '{}/{}'.format(jobs_dir, job_name)
            s3_jobs_dir = '/'.join(s3_key.split('/')[0:2])
                
                
            
            # Launch annotation job as a background process
            # ref doc of subprocess: https://docs.python.org/3/library/subprocess.html
            # Run the AnnTools command
            try:
                ann_process = subprocess.Popen(
            ['sh', '-c', 'cd {} && python run.py {} {}'.format(base_dir, local_file_abs_dir, s3_jobs_dir)]
            )
            except subprocess.CalledProcessError as e:
                print("'An error occurred in the annotator process. Please review the input data and try again:", e)
                return

                    
            try:
                dynamodb = boto3.resource("dynamodb",region_name=config['aws']['AwsRegionName'] )
                table = dynamodb.Table(config['gas']['AnnotationsTable'])
                # Update job status in DynamoDB conditionally
            except ClientError as e:
                print (f"Failed connecting to database: {e}")
                return

            try:
                response = table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='SET job_status = :val',
                    ConditionExpression='job_status = :status',
                    ExpressionAttributeValues={':val': 'RUNNING',':status': 'PENDING'},
                    ReturnValues="UPDATED_NEW"
                )
            except BotoCoreError as e:
                # Handle the specific DynamoDB error (e.g., ConditionalCheckFailedException)
                print('DynamoDB error:', e)
                return 
            except ClientError as e:
                print (f"Failed connecting to database: {e}")
                return 
            try:
                # Delete messages
                #ref doc: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/step-receive-delete-message.html
                # Delete the message from the queue after processing
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=config['sqs']['RequestQueueUrl'],
                    ReceiptHandle=receipt_handle
                )
            except ClientError as e:
                print(f"Failed to delete message: {e.response['Error']['Message']}")
                return 


def main():

    # Get handles to queue

    # Poll queue for new results and process them
    while True:
        handle_requests_queue()


if __name__ == "__main__":
    main()

### EOF
