# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
import boto3
import botocore
from botocore import exceptions

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')

db = boto3.client('dynamodb', region_name=config['aws']['AwsRegionName'])
sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

def restore():
    while True:
        try:
            response = sqs.receive_message(
            QueueUrl=config['aws']['AwsSQSRestoreUrl'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
            )

            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            data = json.loads(message['Body'])

        except KeyError:
            # Empty queue
            continue

        # Getting information from body of message
        user_id = data['user_id']
        profile = helpers.get_user_profile(id=user_id)

        # Double checking that the restoration process is initiated by a premium user
        if profile['role'] == 'premium_user':
            try:
                # Grabbing annotations that are archived from Dynamo table
                # Source: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.FilterExpression.html
                response = db.query(
                    TableName=config['aws']['AwsDynamoTable'],
                    IndexName='user_id_index', 
                    Select='SPECIFIC_ATTRIBUTES', 
                    ProjectionExpression='archive_id, s3_key_input_file', 
                    KeyConditionExpression='user_id = :u', 
                    ExpressionAttributeValues={
                        ':u': {'S': user_id}}, 
                    FilterExpression='attribute_not_exists(s3_key_result_file) and attribute_exists(archive_id)' 
                )

                glacier_ids = response['Items']
                for id in glacier_ids:
                    i = id['archive_id']['S']
                    file = id['s3_key_input_file']['S'].split('.')[0]

                    try: 
                        # Expedited Glacier job
                        # Send thaw notification
                        # Source: https://docs.aws.amazon.com/cli/latest/reference/glacier/initiate-job.html
                        response = glacier.initiate_job(
                            vaultName=config['aws']['AwsGlacierVault'], 
                            jobParameters={'Type': 'archive-retrieval',
                                           'ArchiveId': i,
                                           'Description': '{}{}/{}.annot.vcf'.format(config['aws']['AwsPrefix'], user_id, file),
                                           'SNSTopic': config['aws']['AwsSNSThawARN'], 
                                           'Tier': 'Expedited'
                                           }
                        )

                    # Source: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/exceptions/InsufficientCapacityException.html
                    except glacier.exceptions.InsufficientCapacityException:
                        # Standard Glacier job if there's limited capacity
                        # Send thaw notification
                        response = glacier.initiate_job(
                            vaultName=config['aws']['AwsGlacierVault'], 
                            jobParameters={'Type': 'archive-retrieval',
                                           'ArchiveId': i, 
                                           'Description': '{}{}/{}.annot.vcf'.format(config['aws']['AwsPrefix'], user_id, file),
                                           'SNSTopic': config['aws']['AwsSNSThawARN'], 
                                           'Tier': 'Standard'
                                           }
                        )
            except exceptions.ClientError as e:
                print({
                    'code': 404,
                    'status': 'error',
                    'message': 'Dynamo table query failed: {}'.format(str(e))
                })
                continue
        else:
            pass

        try:
            sqs.delete_message(
                QueueUrl=config['aws']['AwsSQSRestoreUrl'],
                ReceiptHandle=receipt_handle
                )
        
        except exceptions.ClientError as e:
            print({
                    'code': 500,
                    'status': 'error',
                    'message': 'SQS delete message failed: {}'.format(str(e))
            })
            continue

restore()
### EOF
