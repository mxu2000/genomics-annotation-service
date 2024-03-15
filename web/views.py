# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
import re
import botocore
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)

"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

    # Get bucket name, key, and job ID from the S3 redirect URL
    bucket_name = str(request.args.get('bucket'))
    s3_key = str(request.args.get('key'))

    # Extract the job ID from the S3 key
    pattern = r'^(.+/)([^/]+)/([a-z0-9-]+)~(.+\.vcf)$'
    match = re.match(pattern, s3_key)
    if not match:
        return jsonify({
            'code': 400,
            'status': 'error',
            'message': 'Invalid key format.'
        })

    user = session['primary_identity']
    id = match.group(3)
    filename = match.group(4)

    # Persist job to database
    data = {"job_id": id,
            "user_id": user,
            "input_file_name": filename,
            "s3_inputs_bucket": bucket_name,
            "s3_key_input_file": '{}~{}'.format(id, filename),
            "submit_time": int(time.time()),
            "job_status": "PENDING"
            }

    try:
        db = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
        table = db.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/put_item.html
        table.put_item(Item=data)

    except botocore.exceptions.ClientError:
        return jsonify({
            'code': 500,
            'status': 'error',
            'message': 'Dynamodb Error: File information could not be entered.'
        })

    # Send message to request queue
    try:
        # Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
        sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
        sns.publish(
            TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
            Message=json.dumps(data),
            MessageGroupId='jobRequestsGroup',
            MessageDeduplicationId=id
        )

    except Exception as e:
        return jsonify({
            'code': 500,
            'status': 'error',
            'message': 'SNS Error: {}'.format(str(e))
        })

    return render_template('annotate_confirm.html', job_id=id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    db = boto3.client('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    try:
        # Getting annotations from Dynamo table
        response = db.query(
            TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'],
            IndexName='user_id_index',
            KeyConditionExpression='user_id = :u',
            ExpressionAttributeValues={
                ':u': {'S': session['primary_identity']}
            },
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression="job_id, submit_time, input_file_name, job_status"
        )

    except Exception as e:
        return jsonify({
            'code': 500,
            'status': 'error',
            'message': 'Dynamodb Error: Table query failed. Try again'
        })

    # Formatting annotations
    # Source: https://www.geeksforgeeks.org/python-time-localtime-method/
    cleaned_list = [
        {
            'job_id': item['job_id']['S'],
            'submit_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(item['submit_time']['N']))),
            'input_file_name': item['input_file_name']['S'],
            'job_status': item['job_status']['S']
        }
	for item in response['Items']
    ]

    return render_template('annotations.html', annotations=cleaned_list)

"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    user = session['primary_identity']
    db = boto3.client('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
    free_access_expired = False
    try:
        # Getting information about specific annotation from Dynamo table
        response = db.query(
            TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'],
            KeyConditionExpression='job_id = :j',
            FilterExpression='user_id = :u',
            ExpressionAttributeValues={
                ':j': {'S': id},
                ':u': {'S': user}
            },
            ProjectionExpression='job_id, storage_status, job_status, submit_time, input_file_name, complete_time, s3_key_result_file, s3_key_log_fi$
        )
        # Wrong user
        if not response['Items']:
                  return jsonify({
                      'code': 500,
                      'status': 'error',
                      'message': 'Not authorized to view this job.'
                  })
    except Exception as e:
        return jsonify({
            'code': 500,
            'status': 'error',
            'message': 'Dynamodb Error: Table query failed. Try again'
        })
    # Formatting annotation information
    cleaned_list = [
        {
            'job_id': item['job_id']['S'],
            'submit_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(item['submit_time']['N']))),
            'input_file_name': item['input_file_name']['S'],
            'job_status': item['job_status']['S']
        }
              for item in response['Items']
    ]

    annotation = cleaned_list[0]

    if 'storage_status' not in response['Items'][0].keys():
      annotation['storage_status'] = None
    else:
      annotation['storage_status'] = response['Items'][0]['storage_status']['S']

    filename = annotation['input_file_name'].split('.')[0]
    input_file = '{}{}/{}~{}.vcf'.format(app.config['AWS_S3_KEY_PREFIX'], user, id, filename)
    result_file = '{}{}/{}~{}.annot.vcf'.format(app.config['AWS_S3_KEY_PREFIX'], user, id, filename)

    if annotation['job_status'] == 'COMPLETED' or annotation['storage_status'] == 'RESTORED':
        item = response['Items'][0]
        complete_time = float(item['complete_time']['N'])
        annotation['complete_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(complete_time))
        annotation['s3_key_log_file'] = item['s3_key_log_file']['S']
        complete_sec = time.mktime(time.strptime(annotation['submit_time'], '%Y-%m-%d %H:%M:%S'))
        # Five minute limit for free users to download the results file
        if session['role'] == 'free_user' and time.time() - complete_sec > 300:
            free_access_expired = True
        # File was archived and is being restored from Glacier Vault
        if 's3_key_result_file' not in response['Items'][0].keys() and annotation['job_status'] == 'COMPLETED':
            annotation['restore_message'] = 'This file is currently being restored. Please try again in a few hours.'
	else:
            annotation['s3_key_result_file'] = result_file
            try:
                # Generate download URL for results file
		# Source: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
                result_url = s3.generate_presigned_url(
                    ClientMethod='get_object',
                    Params={
                        'Bucket': app.config['AWS_S3_RESULTS_BUCKET'],
                        'Key': annotation['s3_key_result_file']
                    },
                    ExpiresIn=3600
                )
                annotation['result_file_url'] = result_url

            except Exception as e:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                'message': 'URL could not be generated for input file: {}'.format(str(e))
            })

    return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
    db = boto3.client('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    user = session['primary_identity']
    try:
        # Getting information about specific annotation from Dynamo table
        response = db.query(
            TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'],
            KeyConditionExpression='job_id = :j',
            FilterExpression='user_id = :u',
            ExpressionAttributeValues={
                ':j': {'S': id},
                ':u': {'S': user}
            },
            ProjectionExpression='input_file_name'
        )

	if not response['Items']:
                  return jsonify({
                      'code': 500,
                      'status': 'error',
                      'message': 'Not authorized to view this job.'
                  })

    except Exception as e:
        return jsonify({
            'code': 500,
            'status': 'error',
            'message': 'Dynamodb Error: Table query failed. Try again'
        })
  
    input_file = response['Items'][0]['input_file_name']['S']
    log_file = '{}{}/{}~{}.count.log'.format(app.config['AWS_S3_KEY_PREFIX'], user, id, input_file)
    s3 = boto3.resource('s3', region_name=app.config['AWS_REGION_NAME'])

    try:
        # Getting the contents of the log file
        log_file_contents = s3.Object(app.config['AWS_S3_RESULTS_BUCKET'], log_file).get()['Body'].read().decode('utf-8')

    except Exception as e:
        return jsonify({
            'code': 500,
            'status': 'error',
            'message': 'S3 error:{}.'.format(str(e))
        })

    return render_template('view_log.html', job_id=id, log_file_contents=log_file_contents)

"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
    if (request.method == 'GET'):
        # Display form to get subscriber credit card info
        if (session.get('role') == "free_user"):
            return render_template('subscribe.html')
        else:
            return redirect(url_for('profile'))

    elif (request.method == 'POST'):
        # Update user role to allow access to paid features
        update_profile(
            identity_id=session['primary_identity'],
            role="premium_user"
        )

        # Update role in the session
        session['role'] = "premium_user"

        # Run restore to unarchive files
        # Sending message to restore queue
        message = {
            "user_id": session['primary_identity']
        }
	sqs = boto3.client('sqs', region_name=app.config['AWS_REGION_NAME'])
        sqs.send_message(
            QueueUrl=app.config['AWS_SQS_RESTORE_QUEUE'],
            MessageBody=json.dumps(message)
        )

        # Display confirmation page
        return render_template('subscribe_confirm.html')

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(
        identity_id=session['primary_identity'],
        role="free_user"
    )
    return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if (request.args.get('next')):
        session['next'] = request.args.get('next')
    return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', 
        title='Page not found', alert_level='warning',
        message="The page you tried to reach does not exist. \
            Please check the URL and try again."
        ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html',
        title='Not authorized', alert_level='danger',
        message="You are not authorized to access this page. \
            If you think you deserve to be granted access, please contact the \
            supreme leader of the mutating genome revolutionary party."
        ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
    return render_template('error.html',
        title='Not allowed', alert_level='warning',
        message="You attempted an operation that's not allowed; \
            get your act together, hacker!"
        ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html',
        title='Server error', alert_level='danger',
        message="The server encountered an error and could \
            not process your request."
        ), 500

### EOF
