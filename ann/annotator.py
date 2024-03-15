from configparser import SafeConfigParser
import subprocess
import botocore
import boto3
import json
import os

from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

def request_annotation():
    # Connect to SQS and get the message queue
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    url = config['aws']['AwsSQSRequestsUrl']
    sqs.set_queue_attributes(QueueUrl=url, Attributes={'ReceiveMessageWaitTimeSeconds': '10'})

    # Poll the message queue in a loop
    while True:
        # Attempt to read a message from the queue
        # Use long polling - DO NOT use sleep() to wait between polls
        # Source: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
        # Source: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
        try:
            response = sqs.receive_message(
                QueueUrl=url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            data = json.loads(body['Message'])

        except KeyError:
            # Queue is empty
            continue

        # If message read, extract job parameters from the message body as before
        bucket = data['s3_inputs_bucket']
        path = 'maxinexu/{}/'.format(data['user_id'])
        id = data['job_id']
        filename = data['input_file_name']
        key = '{}{}'.format(path, data['s3_key_input_file'])

        # Include below the same code you used in prior homework
        # Get the input file S3 object and copy it to a local file
        # Use a local directory structure hat makes it easy to organize multiple running annotation jobs
        job_path = './jobs/{}'.format(id)
        os.makedirs(job_path)
        s3_client = boto3.resource('s3', region_name=config['aws']['AwsRegionName'])
        download_path = os.path.join(job_path, filename)
        s3_client.meta.client.download_file(bucket, key, download_path)

        db = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        table = db.Table(config['aws']['AwsDynamoTable'])

        # Check to see if job ID is in the Dynamodb table
        try:
            response = table.get_item(Key={'job_id':id})

        except botocore.exceptions.ClientError:
            print({
                'code': 500,
                'status': 'error',
                'message': 'Dynamodb table was not able to be accessed.'
            })

        if 'Item' not in response:
            print({
                'code': 404,
                'status': 'error',
                'message': 'Job ID was not found in the Dynamodb table'
            })

        try:
        # Launch annotation job as a background process
        # Source: https://docs.python.org/3/library/subprocess.html
        # Source: https://stackoverflow.com/questions/21406887/subprocess-changing-directory
            subprocess.Popen(['sh', '-c', 'python run.py jobs/{id}/{filename} {path} {id} {filename}'.format(id=id, filename=filename, path=path)])
            
            try:
            # Source: https://stackoverflow.com/questions/37053595/how-do-i-conditionally-insert-an-item-into-a-dynamodb-table-using-boto3
                table.update_item(
                    Key= {'job_id': id},
                    UpdateExpression="set job_status = :new_status",
                    ExpressionAttributeValues={
                        ':new_status': 'RUNNING',
                        ':expected_status': 'PENDING'
                        },
                    ConditionExpression='job_status = :expected_status',
                    ReturnValues='ALL_NEW'
                )

            # Source: https://stackoverflow.com/questions/38733363/dynamodb-put-item-conditionalcheckfailedexception
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    print({
                        'code': 400,
                        'status': 'error',
                        'message': 'Job status is not currently pending.'
                    })

                else:
                    print({
                        'code': 500,
                        'status': 'error',
                        'message': 'Status could not be updated. Please try again.'
                    })

        # Source: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status#client_error_responses
        except subprocess.CalledProcessError:
            return print({
                'code': 500,
                'status': 'error',
                'message': 'Failed annotation attempt. Please try again.'
            })


        # Delete the message from the queue, if job was successfully submitted
        sqs.delete_message(QueueUrl=url, ReceiptHandle=receipt_handle)

        print({
            "code": 201,
            "data": {
                "job_id": id,
                "input_file": filename,
            }
	})

request_annotation()
# EOF
