# -*- coding: utf-8 -*-

import json
import pickle
import base64
import httplib2
import os
import datetime
import uuid

from google.cloud import pubsub_v1
from google.cloud.storage import Client, Blob
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
import google.cloud.exceptions


# GLOBALS
#
FB_ADMIN_SDK_BUCKET                     = os.environ.get("FB_ADMIN_SDK_BUCKET")
FB_ADMIN_SDK_CREDENTIALS_PATH           = os.environ.get("FB_ADMIN_SDK_CREDENTIALS_PATH")
PS_COMPOSER_DAG_TRIGGER_GCP_PROJECT_ID  = os.environ.get("PS_COMPOSER_DAG_TRIGGER_GCP_PROJECT_ID")
PS_COMPOSER_DAG_TRIGGER_TOPIC           = os.environ.get("PS_COMPOSER_DAG_TRIGGER_TOPIC")


def get_firebase_admin_sdk_service_account_credentials():

    # Read file from GCS
    #
    # Instantiate Google Bigquery client
    #
    try:
        gcs_client = Client()
        bucket = gcs_client.get_bucket(FB_ADMIN_SDK_BUCKET)
        blob = Blob(FB_ADMIN_SDK_CREDENTIALS_PATH, bucket)
        json_credentials = json.loads(blob.download_as_string())
        return credentials.Certificate(json_credentials)
    except Exception as ex:
        print("Cannot retrieve FB Admin SDK service account credentials")
        print(ex)
        return None


def get_firebase_app(name="[DEFAULT]"):

    # Try to get the app
    #
    try:
        app = firebase_admin.get_app(name=name)
        return app
    except ValueError as ex:
        print("Firebase APP does not exists.")

    # Initialize a new app
    #
    fb_creds = get_firebase_admin_sdk_service_account_credentials()
    if fb_creds is None:
        print("Error while retrieving FB Admin SDK account credentials.")
        return None
    try:
        app = firebase_admin.initialize_app(credential=fb_creds, name=name)
        print("Firebase APP freshly initialized.")
        return app
    except ValueError as ex:
        print("Firebase APP cannot be initialized.")
        return None


def get_user_accounts(user_id):

    try:
        default_app = get_firebase_app()

        # Get Accounts from claims
        #
        user = auth.get_user(user_id)
        user_accounts = user.custom_claims.get("accounts")

        # TESTING
        # Get roles
        #
        user_roles = user.custom_claims.get("studioRoles")
        print(user_roles)

        print("User Id       : {}".format(user_id))
        print("User accounts : {}".format(user_accounts))

        return user_accounts

    except Exception as ex:

        print("Error while getting user accounts.\n{}".format(ex))
        return None


def send_dag_trigger_to_pubsub(gcp_project_id, pubsub_topic, dag_id, dag_data):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_project_id, pubsub_topic)

    payload = {}
    payload["dag_id"]       = dag_id
    payload["dag_conf"]     = dag_data
    payload["dag_run_id"]   = datetime.datetime.today().strftime("%Y%m%d-%H%M%S") + "-" + str(uuid.uuid4())

    data = bytes(json.dumps(payload), "utf-8")

    message_future = publisher.publish(topic_path, data=data)


def process_post_request(request_dict, user_id):

    # First, let's check if we have the right data in the request
    #
    # request_dict["data"]["dagId"].stri
    if "dagId" not in request_dict["data"]:
        message = "Request is missing an attribute : dagId"
        print(message)
        return({"data": message}, 400)

    if "dagConf" not in request_dict["data"]:
        message = "Request is missing an attribute : dagConf"
        print(message)
        return({"data": message}, 400)

    # Let's check if the user requesting the logs has access to the DAGs run account
    #
    user_accounts = None
    try:
        user_accounts = get_user_accounts(user_id=user_id)
    except Exception as ex:
        print("Exception occurred while getting user accounts.\n{}".format(ex))

    if user_accounts is None:
        message = "No user accounts found."
        print(message)
        return({"data": message}, 403)

    # Send to PubSub
    #

    try:

        send_dag_trigger_to_pubsub( gcp_project_id=PS_COMPOSER_DAG_TRIGGER_GCP_PROJECT_ID,
                                    pubsub_topic=PS_COMPOSER_DAG_TRIGGER_TOPIC,
                                    dag_id=request_dict["data"]["dagId"].strip(),
                                    dag_data=request_dict["data"]["dagConf"])

        return({"data": "DAG {} set to be triggered.".format(request_dict["data"]["dagId"].strip())}, 200)

    except Exception as ex:
        print("Error while processing : {}".format(ex))
        return {"message": "Error while requesting DAG trigger."}, 500

    
def process(incoming_request):

    print("PROCESSING ...")

    # Set CORS headers for the main request
    #
    cors_headers = {
        'Access-Control-Allow-Origin': '*'
    }

    http_method = None
    request_dict = None
    try:

        http_method = incoming_request.method.strip()
        print("HTTP Method : {}".format(http_method))

        if http_method == 'OPTIONS':

            headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'true',
                'Access-Control-Max-Age': '3600'
            }

            return ('', 204, headers)

        auth_token = ((incoming_request.headers["Authorization"]).strip()).replace(
            "Bearer ", "")

    except Exception as ex:
        print(type(ex))
        print(ex)
        data = {}
        data["message"] = "Error while parsing request."
        return (json.dumps(data), 500, cors_headers)

    # Check USER token
    #
    try:
        default_app = get_firebase_app()
        decoded_token = auth.verify_id_token(auth_token, app=default_app)

        # Get USER ID
        #
        user_id = decoded_token['uid']

    except Exception as ex:

        print(type(ex))
        print(ex)
        data = {}
        data["message"] = "Error while authenticating user."
        return (json.dumps(data), 403, cors_headers)

    # Route request according to HTTP verb.
    #
    payload = None
    http_status = None

    if http_method == "POST":

        request_dict = incoming_request.get_json()
        # print(request_dict)

        # Check query parameters
        #
        payload, http_status = process_post_request(request_dict, user_id)

        return (json.dumps(payload), http_status, cors_headers)

    else:
        return (json.dumps({"message": "Method not allowed."}), 405, cors_headers)
