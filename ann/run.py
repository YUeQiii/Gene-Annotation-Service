# run.py
#
# Runs the AnnTools pipeline
#
# NOTE: This file lives on the AnnTools instance and
# replaces the default AnnTools run.py
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import sys
import time
import driver

import boto3
import os 
import botocore
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import json

# /home/ubuntu/gas/ann/
base_dir = os.path.abspath(os.path.dirname(__file__))

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read(os.path.join(base_dir, "annotator_config.ini"))

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == '__main__':

    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
	        driver.run(sys.argv[1], 'vcf')

        result_bucket = config['s3']['ResultsBucketName']
        # example of argv[1]: '/home/ubuntu/gas/ann/userX/12234566~filename'
        # example of argv[1]: '/home/ubuntu/jobs/userX~12234566~filename'
        arguments = str(sys.argv[1]).split('/')
        job_name = str(arguments[-1])
        job_id = str(job_name.split('~')[0])
        file_name = str(job_name.split('~')[1])
        user_id=str(arguments[-2])
        job_prefix = job_name.partition('.')[0]

        # Validate job details
        if not job_id or not job_name or not job_prefix:
            print('Invalid file path')

        # example of jobs_dir (in ann server): /home/ubuntu/gas/ann/userX
        jobs_dir = "{}/{}".format(base_dir, user_id)

        # example of result dir: yueqil/userX
        result_dir = sys.argv[2] 

        # check whether the path is correct
        if not job_id or not job_name or not job_prefix or not result_dir:
            print('Invalid file path')

        s3_client = boto3.client('s3', region_name = config['aws']['AwsRegionName'], config=botocore.client.Config(signature_version=config['aws']['SignatureVersion']))

        # File names for results and logs
        result_file_name = f"{job_prefix}.annot.vcf"
        log_file_name = f"{job_prefix}.vcf.count.log"

        # Define S3 keys
        s3_key_result_file = f"{result_dir}/{result_file_name}"
        s3_key_log_file = f"{result_dir}/{log_file_name}"

        # 1. Upload the files to S3 results bucket
        # upload API ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
        # ref doc: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        try:
            s3_client.upload_file(f"{jobs_dir}/{result_file_name}", result_bucket, s3_key_result_file)
            s3_client.upload_file(f"{jobs_dir}/{log_file_name}", result_bucket, s3_key_log_file)
        except ClientError as e:
            print(f"Failed uploading result file: {e}")
            sys.exit(1)

        # 2. Update DynamoDB  
        #ref doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html
        #ref doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
        try:
            dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
            table = dynamodb.Table(config['gas']['AnnotationsTable'])
        except ClientError as e:
            print (f"Failed connecting to database: {e}")

        try:
            complete_time = int(time.time())
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='SET job_status = :status, s3_results_bucket = :rb, s3_key_result_file = :rf, s3_key_log_file = :lf, complete_time = :ct',
                ExpressionAttributeValues={
                    ':status': 'COMPLETED',
                    ':rb': result_bucket,
                    ':rf': s3_key_result_file,
                    ':lf': s3_key_log_file,
                    ':ct': complete_time
                },
                ReturnValues = "UPDATED_NEW"   
            )
        except ClientError as e:
            print(f"Failed to update DynamoDB: {e}")

        # 3. Clean up local job files
        try:
            os.remove(f"{jobs_dir}/{job_name}")
            os.remove(f"{jobs_dir}/{result_file_name}")
            os.remove(f"{jobs_dir}/{log_file_name}")
        except OSError as e:
            print(f"Error during file cleanup: {e}")

    else:
        print("A valid .vcf file must be provided as input to this program.")

