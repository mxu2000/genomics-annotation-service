# archive.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json

from botocore import exceptions

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
db = boto3.client('dynamodb', region_name=config['aws']['AwsRegionName'])
sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

# Add utility code here
def archive():
    while True:
        response = sqs.receive_message(
            QueueUrl=config['aws']['AwsSQSArchiveUrl'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        try:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            data = json.loads(message['Body'])
        
        except KeyError:
            # Empty queue
            continue
        
        # Getting information from body of message
        user_id = data['user_id']
        job_id = data['job_id']
        s3_key_result_file = data['s3_key_result_file']

        # Premium users' files should not be archived
        profile = helpers.get_user_profile(id=user_id)
        if profile['role'] == 'premium_user':
            sqs.delete_message(
                QueueUrl=config['aws']['AwsSQSArchiveUrl'],
                ReceiptHandle=receipt_handle
            )
            continue

        try:
            # Getting information from file in the S3 bucket
            response = s3.get_object(
                Bucket=config['aws']['AwsS3ResultsBucket'],
                Key=s3_key_result_file)
            file_body = response['Body'].read()
        except exceptions.ClientError as e: 
            print('Error getting results file from S3: {}'.format(s3_key_result_file))
            continue

        # Uploading file to Glacier Vault
        # Source: https://docs.aws.amazon.com/cli/latest/reference/glacier/upload-archive.html
        response = glacier.upload_archive(
            vaultName=config['aws']['AwsGlacierVault'],
            body = file_body)

        archive_id = response['archiveId']

        try:
            # Updating Dynamo table (removing s3_key_result_file from DynamoTable)
            db.update_item(
                TableName=config['aws']['AwsDynamoTable'], 
                Key={'job_id': {'S': job_id}}, 
                ExpressionAttributeValues={
                    ':id': {'S': archive_id}
                }, 
                UpdateExpression='SET archive_id = :id REMOVE s3_key_result_file'
            )
        
        except exceptions.ClientError as e:
            print({
                'code': 500,
                'status': 'error',
                'message': 'Dynamo table could not be updated: {}'.format(str(e))
            })
            continue

        try:
            # Deleting file from S3 bucket
            s3.delete_object(
                Bucket=config['aws']['AwsS3ResultsBucket'],
                Key=s3_key_result_file
            )
        
        except exceptions.ClientError as e:
            print({
                'code': 500,
                'status': 'error',
                'message': 'File could not be deleted from S3 Bucket: {}'.format(str(e))
            })
            continue

        try:
            # Deleting message from queue
            sqs.delete_message(
                QueueUrl=config['aws']['AwsSQSArchiveUrl'],
                ReceiptHandle =receipt_handle
            )
        
        except exceptions.ClientError:
            print({
                'code': 500,
                'status': 'error',
                'message': 'SQS Archive message could not be deleted from S3 Bucket: {}'.format(str(e))
            })

archive()
# EOF
