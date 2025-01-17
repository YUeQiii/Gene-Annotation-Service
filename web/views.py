# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for

from app import app, db
from decorators import authenticated, is_premium

from auth import get_profile

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version=app.config['AWS_S3_SIGNATURE_VERSION']),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
def create_annotation_job_request():

    region = app.config["AWS_REGION_NAME"]

    # Parse redirect URL query parameters for S3 object info
    bucket_name = request.args.get("bucket") # gas-input
    s3_key = request.args.get("key") # CNetID/userX/uuid~filename

    # Extract the job ID from the S3 key
    job_id = str(s3_key.split('/')[2].split('~')[0]) # uuid
    user_id = str(s3_key.split('/')[1]) # userX
    file_name = str(s3_key.split('~')[1]) # filename 

        
    # Convert current time to epoch time (in seconds)
    submit_time = int(time.time())

    # DynamoDB setup
    #ref doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.SDKOverview.html
    #ref doc: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html


    if(len(file_name) >1 and len(job_id)>1):
        # Create a job item for DynamoDB
        item ={
            "job_id":job_id,
            "user_id": user_id,
            "input_file_name": file_name,
            "s3_input_bucket": bucket_name, 
            "s3_key_input_file" : s3_key,  
            "submit_time": submit_time,
            "job_status":"PENDING"
        }
        try:
            dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
            table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
        except ClientError as e:
            app.logger.error(f"Failed connecting to database: {e}")
            return abort(500)
        
        # Persist job to database
        # ref doc of putting item into table:https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/put_item.html#put-item
        try:
            table.put_item(Item = item)
        except ClientError as e:
            app.logger.error(f"Failed putting item into table: {e}")
            return abort(500)

        # Send message to request queue
        # ref of sending SNS message: https://docs.aws.amazon.com/code-samples/latest/catalog/python-sns-sns-python-example-publish-to-topic.py.html
        # ref doc: https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
        # ref doc: https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html#publish-to-topic-aws-sdks
        try:
            # Create an SNS client
            sns_client = boto3.client('sns',region_name=app.config["AWS_REGION_NAME"])

            # Convert the message data to a string format (JSON) to publish
            message = json.dumps(item)

            # SNS topic ARN
            sns_topic_arn = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]

            # Publish the message 
            response = sns_client.publish(
                TopicArn = sns_topic_arn,
                Message = message,
                Subject = 'Job Request Notification'
            )

        except ClientError as e:
            app.logger.error(f"Failed publishing job to message queue: {e}")
            return abort(500)

        return render_template("annotate_confirm.html", job_id=job_id)
    
    else :
        app.logger.error("No valid file selected, please resubmit.")
        return abort(500)



"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
def annotations_list():

    # Get list of annotations to display

    return render_template("annotations.html", annotations=None)


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
def annotation_details(id):
    pass


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
def annotation_log(id):
    pass


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info

        # If A15 not completed, force-upgrade user role and initiate restoration
        pass

    elif request.method == "POST":
        # Process the subscription request

        # Create a customer on Stripe

        # Subscribe customer to pricing plan

        # Update user role in accounts database

        # Update role in the session

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # ...and make sure you handle files pending archive!

        # Display confirmation page
        pass


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
