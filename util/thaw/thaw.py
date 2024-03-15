# thaw.py
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
config.read('thaw_config.ini')

db = boto3.client('dynamodb', region_name=config['aws']['AwsRegionName'])
dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

def thaw():
    while True:
        response = sqs.receive_message(
            QueueUrl=config['aws']['AwsSQSThawUrl'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        try:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            data = json.loads(body['Message'])

        except KeyError:
            # Empty queue
            continue
        
        # Getting information from body of message
        retrieval_id = data['JobId']
        archive_id = data['ArchiveId']
        filename = data['JobDescription']
        s_index = filename.rfind("/") + 1
        e_index = filename.find("~")
        job_id = filename[s_index:e_index]

        # Getting body of file
        # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
        job = glacier.get_job_output(
            accountId='-',
            vaultName=config['aws']['AwsGlacierVault'],
            jobId=retrieval_id,
        )
        message_body = job['body'].read()

        try:
            # Uploading to S3 results bucket
            # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
            s3.put_object(
                Body=message_body,
                Bucket=config['aws']['AwsS3ResultsBucket'],
                Key=filename
            )

        # Source: https://github.com/boto/boto3/issues/3055
        except boto3.exceptions.S3UploadFailedError as e:
            print({
                'code': 500,
                'status': 'Server Error',
                'message': 'S3 could not upload file: {}'.format(str(e)),
            })
            continue

        try:
            # Updating status in Dynamo table
            table = dynamo.Table(config['aws']['AwsDynamoTable'])
            table.update_item(Key= {'job_id': str(job_id)},
                    UpdateExpression="SET storage_status = :ss, archive_id = :a,  s3_key_result_file = :filename",
                    ExpressionAttributeValues={
                        ':ss': 'RESTORED',
                        ':a': '',
                        ':filename': filename
                        },
                ReturnValues="UPDATED_NEW"
                )

        except exceptions.ClientError as e:
            print({
                'code': 404,
                'status': 'error',
                'message': 'Error updating DynamoTable, file not found: {}'.format(str(e))
            })
            continue

        try:
            sqs.delete_message(
                QueueUrl=config['aws']['AwsSQSThawUrl'],
                ReceiptHandle=receipt_handle
                )

        except exceptions.ClientError as e:
            print({
                'code': 500,
                'status': 'error',
                'message': 'SQS Error: Thaw message could not be deleted: {}'.format(str(e))
            })
        
        try:
            # Delete file from Glacier vault
            # Source: https://docs.aws.amazon.com/cli/latest/reference/glacier/delete-archive.html
            glacier.delete_archive(
                accountId='-',
                vaultName=config['aws']['AwsGlacierVault'],
                archiveId=archive_id
            )
        
        except exceptions.ClientError as e:
            print({
                'code': 500,
                'status': 'error',
                'message': 'Glacier Error: {}'.format(str(e))
            })

        print("File was successfully thawed")

thaw()
### EOF
