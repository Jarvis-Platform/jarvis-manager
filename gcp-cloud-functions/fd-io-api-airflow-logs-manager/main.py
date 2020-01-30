# -*- coding: utf-8 -*-

import json
import pickle
import base64
import httplib2
import os

from google.cloud.storage import Client, Blob
import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials
from google.cloud import firestore
import google.cloud.exceptions


# GLOBALS
#
FB_ADMIN_SDK_BUCKET = os.environ.get("FB_ADMIN_SDK_BUCKET")
FB_ADMIN_SDK_CREDENTIALS_PATH = os.environ.get("FB_ADMIN_SDK_CREDENTIALS_PATH")


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


def check_dag_run_vs_accounts(accounts, dag_type, dag_run_id):

    # Some info
    #
    print("DAG type requested : {}".format(dag_type))
    print("DAG Run Id         : {}".format(dag_run_id))

    # Parse dag_type for proper collection
    #
    collections = {
        "storage-to-storage" : "storage-to-storage-runs",
        "gbq-to-gbq" :  "gbq-to-gbq-runs",
        "storage-to-tables" : "storage-to-tables-runs"
    }

    try:
        db = firestore.Client(project="fd-jarvis-datalake")

        doc = db.collection(collections[dag_type]).document(dag_run_id).get()

        if doc.exists is True:

            doc_dict = doc.to_dict()
        
            dag_account = doc_dict["account"].strip()
            print("DAG account : {}".format(dag_account))

            if dag_account in accounts:
                return True
            else:
                print("No match between user's authorized Accounts and DAG account.")
                return False 

        else:
            print("No document corresponding to the DAG run found.")
            return False

    
        return True

    except Exception as ex:
        print("Error while checking Accounts VS DAG run.\n{}".format(ex))
        return False


def process_post_request(request_dict, user_id):

    # First, let's check if the user requesting the logs has access to the DAGs run account
    #
    user_accounts = None
    try:
        user_accounts = get_user_accounts(user_id=user_id)
    except Exception as ex:
        print("Exception occured while getting user accounts.\n{}".format(ex))

    if user_accounts is None:
        message = "No user accounts found."
        print(message)
        return({"data": message}, 403)

    # Check that the DAG run requested belong to the user's account
    #
    check_dag_run = False
    try:
        check_dag_run = check_dag_run_vs_accounts(
            accounts=user_accounts,
            dag_type=request_dict["data"]["dagType"].strip(),
            dag_run_id=request_dict["data"]["dagRunId"].strip())

    except Exception as ex:
        print("Exception occured while checking DAG run VS User account.\n{}".format(ex))

    if check_dag_run is False:
        message = "User does not have access to DAG account."
        print(message)
        return({"data": message}, 403)

    try:

        # Read the data from Google Cloud Storage
        read_storage_client = Client()

        # Set buckets and filenames
        bucket_name = "europe-west1-fd-io-composer-e5bff15e-bucket"

        # get bucket with name
        #
        bucket = read_storage_client.get_bucket(bucket_name)

        blob_prefix = "logs/{}/{}/{}/".format(
            request_dict["data"]["dagId"].strip(),
            request_dict["data"]["taskId"].strip(),
            request_dict["data"]["dagExecutionDate"].strip())

        print("Getting logs : {}".format(blob_prefix))

        blobs = read_storage_client.list_blobs(bucket, prefix=blob_prefix)

        data = {}
        data["data"] = {}

        for blob in blobs:

            try:
                blob_short_name = ((blob.name).strip()).rpartition("/")[2]

                if not blob_short_name:
                    print("Not a file name : {}".format(blob.name))
                    continue

                print("Log filename : {}".format(blob_short_name))

            except Exception:
                # Not a filename.
                #
                print("Not a file name : {}".format(blob.name))
                continue

            try:
                data["data"][blob_short_name] = str(
                    base64.b64encode(blob.download_as_string()), "utf-8")
            except Exception as ex:
                print("Error while processing : {}\n{}".format(blob.name), ex)
                data["data"][blob_short_name] = str(
                    base64.b64encode(b"Error while retrieving logs."), "utf-8")

        if not data["data"]:
            # Empty => not found
            return data, 404
        else:
            return data, 200

    except Exception as ex:
        print("Error while processing : {}".format(ex))
        return {"message": "Error while fetching log files."}, 500


def process(incoming_request):

    print("PROCESSING ...")

    # Set CORS headers for the main request
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
