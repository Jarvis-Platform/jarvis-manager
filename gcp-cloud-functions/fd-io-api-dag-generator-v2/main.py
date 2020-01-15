# -*- coding: utf-8 -*-

import json
import pickle
import base64
import httplib2

from google.cloud import storage
from google.oauth2 import service_account
from google.auth import transport

from fd_io_python_libs.fd_io_firestore_v2 import fd_io_firestore
from fd_io_python_libs.fd_io_gcp_storage import fd_io_gcp_storage


def get_help():

    return {"help": """
Jarvis GCP Cloud Function manager HELP
--------------------------------------

Usage: jarvis dag-generator TABLE-TO-TABLE.json

"""
    }


def process_post_request(request_dict):

    try:
        resource = request_dict["payload"]["resource"].strip()

        if resource == "help":
            return get_help(), 200
        else:
            return {}, 200

    except KeyError as ex:
        print(ex)
        return {}, 500


def process_put_request(request_dict):

    data = {}

    try:
        resource = request_dict["payload"]["resource"]
        project_profile = request_dict["payload"]["project_profile"]
        uid = request_dict["payload"]["uid"]
        dag_data = request_dict["payload"]["dag_file"]["data"]
        dag_filename = request_dict["payload"]["dag_file"]["name"]

        # Get GCP Project ID of the Composer Instance from project profile
        #
        composer_gcp_project_id = fd_io_firestore.get_composer_gcp_project_id_from_project_profile(project_profile)

        # Get Composer Bucket from project profile
        #
        gcp_composer_bucket = fd_io_firestore.get_composer_bucket_from_project_profile(project_profile)

        # Decode resource : configuration associated with the DAG
        #
        decoded_resource = bytes(resource, "utf-8")
        decoded_resource = base64.b64decode(decoded_resource)
        unpickled_resource = pickle.loads(decoded_resource)

        # Decode data : will be uploaded as PY file to the bucket
        #
        decoded_dag_data = bytes(dag_data, "utf-8")
        decoded_dag_data = base64.b64decode(decoded_dag_data)
        unpickled_dag_data = pickle.loads(decoded_dag_data)

        # Uploading configuration
        #
        ret_code, message = fd_io_firestore.deploy_configuration(unpickled_resource, composer_gcp_project_id, uid)

        if ret_code is not True:
            data["message"] = message
            http_status = 500
            return data, http_status

        # Uploading DAG file
        #
        ret_code, message = fd_io_gcp_storage.upload_to_gcs_from_file(unpickled_dag_data,"dags/" + dag_filename, composer_gcp_project_id, gcp_composer_bucket, uid)

        if ret_code is not True:
            data["message"] = message
            http_status = 500
            return data, http_status

        data["message"] = "DAG file and configuration successfully deployed."
        http_status = 200
        return data, http_status


    except Exception as ex:
        print("Error while processing PUT request : %s" % ex)
        data["message"] = "Error while parsing PUT  request %s" % ex
        return data, 500


def process(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        #flask.Flask.make_response>`.
        `make_response <http://flask.pocoo.org/docs/1.0/api/

        Incoming Request Data


    """

    print("PROCESSING ...")

    # Default return value
    #
    data = {}
    data["payload"] = "Configuration received."

    http_method = None
    request_dict = None
    try:

        http_method = request.method.strip()
        request_dict = request.get_json()

        current_object = request._get_current_object()

    except Exception as ex:

        print(ex)
        data["message"] = "Error while parsing request."
        return json.dumps(data)

    # Route request according to HTTP verb.
    #
    payload = None
    http_status = None
    if http_method == "POST":
        payload, http_status = process_post_request(request_dict)
    elif http_method == "PUT":
        payload, http_status = process_put_request(request_dict)
    else:
        return ("Action / resource not found.", 404)

    # Return final info
    #
    if http_status == 200:
        data["payload"] = payload
        return json.dumps(data)
    elif http_status == 403:
        return ("Forbidden, you do have enough permission for that action.", http_status)
    else:
        return (payload["message"], http_status)
