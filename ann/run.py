# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import boto3
import shutil
import botocore
import json
import os
import re

sys.path.append('/home/ec2-user/mpcs-cc/gas/ann/anntools')
import driver

sys.path.insert(1, '/home/ec2-user/mpcs-cc/gas/util')
import helpers

from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

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

        # Initialize the S3 client
        s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
        # Source: https://www.knowledgehut.com/blog/programming/sys-argv-python-examples
        path = sys.argv[2]
        id = sys.argv[3]
        name = sys.argv[4].split('.')[0]
        filename = '{}~{}'.format(id, name)

        # 1. Upload the results file
        # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
        s3.upload_file('./jobs/{}/{}.annot.vcf'.format(id, name), config['aws']['AwsS3ResultsBucket'], '{}{}.annot.vcf'.format(path, filename))

        # 2. Upload the log file
        s3.upload_file('./jobs/{}/{}.vcf.count.log'.format(id, name), config['aws']['AwsS3ResultsBucket'], '{}{}.vcf.count.log'.format(path, filename))

        # Change status to completed and add other information to dynamodb table
        db = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        table = db.Table(config['aws']['AwsDynamoTable'])

        try:
            # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
            table.update_item(
                Key= {'job_id': id},
                UpdateExpression="set job_status = :new_status, s3_results_bucket = :results_bucket, s3_key_result_file = :results_file, s3_key_log_file = :results_log, complete_time = :time",
                ExpressionAttributeValues={
                    ':new_status': 'COMPLETED',
                    ':results_bucket': config['aws']['AwsS3ResultsBucket'],
                    ':results_file': '{}{}~{}.annot.vcf'.format(path, id, name),
                    ':results_log': '{}{}~{}.vcf.count.log'.format(path, id, name),
                    ':time': int(time.time())},
                ReturnValues='ALL_NEW'
            )
        
        except botocore.exceptions.ClientError:
            print ('Status could not be updated. Please try again.')
        
        user_id=table.get_item(Key ={'job_id': id})['Item']['user_id']

        profile = helpers.get_user_profile(id=user_id)

        # Information for email lambda function
        sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
        message = {
            "job_id": id,
            "user_name": profile['name'],
            "user_email": profile['email'],
            "job_status": "COMPLETED"
        }

        # Publish SNS for results queue
        try:
            # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
            sns.publish(
                TopicArn=config['aws']['AwsSNSResultsARN'],
                Message=json.dumps(message),
                MessageGroupId='jobRequestsGroup',
                MessageDeduplicationId=id
                )

        except Exception as e:
            print({
                'code': 500,
                'status': 'error',
                'message': 'SNS Error: {}'.format(str(e))
            })
        
        sqs_message = {
            "job_id": id,
            "user_name": user_id,
            "s3_key_result_file" : "{}{}~{}.annot.vcf".format(path, id, name)
        }

        # Free users have a download limit of 5 minutes, the queue has a delayed delivery of 5 minutes to account for this
        if profile['role'] == 'free_user':
            sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
            sqs.send_message(
                QueueUrl=config['aws']['AwsSQSArchiveUrl'],
                MessageBody=json.dumps(sqs_message)
            )

        try:
        # 3. Clean up (delete) local job files
        # Source: https://www.scaler.com/topics/delete-directory-python/
            shutil.rmtree('./jobs/{}'.format(id))
        except:
            print('File does not exist so it cannot be deleted.')

    else:
	    print("A valid .vcf file must be provided as input to this program.")

### EOF
