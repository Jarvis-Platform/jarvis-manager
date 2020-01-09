# -*- coding: utf-8 -*-

import json
import pickle
import base64
import httplib2
import logging
import copy
from jsonschema import Draft7Validator, validate
from jsonschema.exceptions import ValidationError

from google.cloud import storage
from google.oauth2 import service_account
from google.auth import transport

from fd_io_python_libs.fd_io_firestore_v2 import fd_io_firestore
from fd_io_python_libs.fd_io_firebase import fd_io_firebase_admin_sdk
from fd_io_python_libs.fd_io_gcp_cf_v2 import fd_io_gcp_cf


def decode_user_credentials(user_credentials):

    decoded_credentials = base64.decodebytes(
        bytearray(user_credentials, "utf-8"))
    fully_decoded_credentials = pickle.loads(decoded_credentials)

    return fully_decoded_credentials


def get_help(resource):

    if resource == "deploy_help":

        return {"help": """
Jarvis Configuration manager HELP
---------------------------------

Usage: jarvis [--no-gcp-cf-deploy] deploy configuration YOUR-CONFIGURATION-FILE.json

Current "configuration_type" supported :

    - storage-to-storage
    - workflow
    - vm-launcher
    - gbq-to-gcs
    - storage-to-table (mirror-exc-gcs-to-gbq-conf)
    - storage-to-tables
    - gcs-to-gcs
    - table-to-storage

The flag "--no-gcp-cf-deploy", if set, will prevent any eventual GCP Cloud Function deployment.

"""
                }

    elif resource == "create_help":

        return {"help": """
Jarvis Configuration manager HELP
---------------------------------

Usage: jarvis create CONFIGURATION_TYPE [output_filename.json]

This command will create a file with the specified CONFIGURATION_TYPE template.

Available CONFIGURATION_TYPE:

* storage-to-storage
* storage-to-table

Note that if you do not specify an output filename, the generated file will be created in the current directory and named : CONFIGURATION_TYPE.json

"""
                }

    else:

        return {"help": "No help available for this command."}


def check_configuration(configuration):

    # Retrieve configuration JSON schema according to the "configuration_type"
    #
    try:
        configuration_json_schema = fd_io_firestore.get_document(
            "json-schema", configuration["configuration_type"].strip())
        if configuration_json_schema is None:
            return {"message": "Error while retrieving json schema for configuration type : {}".format(configuration["configuration_type"].strip())}, 404

        errors = ""
        v = Draft7Validator(configuration_json_schema)
        for error in sorted(v.iter_errors(configuration), key=str):

            abs_path = ""
            for item in error.absolute_path:
                abs_path += "[{}]".format(str(item))

            errors += "JSON Path => {} : {}\n".format(abs_path, error.message)

        if errors != "":
            logging.info(errors)
            return {"message": errors}, 400

        return {"message": "Your configuration file is valid."}, 200

    except KeyError:
        logging.info("Error while checking configuration : \n{}".format(configuration))
        return {"message": "\"configuration_type\" not found in the configuration file."}, 500
    except Exception as ex:
        logging.info("Error while checking configuration : \n{}".format(configuration))
        logging.info("Error while checking configuration : {}".format(ex))
        return {"message": "Error while checking configuration : {}".format(ex)}, 500


def get_project_profile_from_configuration(configuration):

    try:

        logging.info("Looking for project profile (\"gcp_project_id\") in configuration for type : {}".format(configuration["configuration_type"]))

        project_profile = None

        if configuration["configuration_type"] in ["storage-to-storage", "storage-to-tables"]:
            if configuration["source"]["type"] == "gcs":
                if configuration["source"]["gcp_project_id"] != "":
                    project_profile = fd_io_firestore.get_project_profile_from_gcp_project_id(configuration["source"]["gcp_project_id"])

        elif configuration["configuration_type"] == "storage-to-table":
            if configuration["gcp_project_id"] != "":
                project_profile = fd_io_firestore.get_project_profile_from_gcp_project_id(configuration["gcp_project_id"])

        if project_profile is not None:
            logging.info("Project profile (\"gcp_project_id\") found in configuration : {}".format(project_profile))
            return {"message": project_profile}, 200
        else:
            return {"message": "GCP Project ID not found."}, 404

    except Exception as ex:
        return {"message": "GCP Project ID not found."}, 404


def get_configuration_template(configuration_type):

    doc = fd_io_firestore.get_document(
        collection="configuration-templates", document=configuration_type)
    if doc is not None:
        return doc, 200
    else:
        return {"message": "Configuration template for type {} not found.".format(configuration_type)}, 400


def process_post_request(request_dict):

    try:

        # Get HELP
        #
        if request_dict["payload"]["resource_type"].strip() == "help":
            return get_help(request_dict["payload"]["resource"].strip()), 200

        # Retrieve CONFIGURATION TEMPLATE
        #
        elif request_dict["payload"]["resource_type"].strip() == "configuration-type":
            return get_configuration_template(request_dict["payload"]["resource"].strip())

        # Manage "check configuration"
        #
        elif request_dict["payload"]["resource_type"].strip() == "check-configuration":
            return check_configuration(request_dict["payload"]["resource"])

        # Manage "retrieve GCP Project ID"
        #
        elif request_dict["payload"]["resource_type"].strip() == "get-gcp-project-id":
            return get_project_profile_from_configuration(request_dict["payload"]["resource"])

        else:
            return {"message": "Resource type unknown."}, 400

    except KeyError as ex:
        print(ex)
        return {}, 500


def process_put_request(request_dict):

    data = {}

    try:
        resource = request_dict["payload"]["resource"]
        project_profile = request_dict["payload"]["project_profile"]
        uid = request_dict["payload"]["uid"]
        deploy_cf = request_dict["payload"]["deploy_cf"]

        # Get Firestore Project ID.
        # This is where the configuration is going to get deployed
        #
        firestore_project_id = fd_io_firestore.get_firestore_project_id_from_project_profile(
            project_profile)

        # Deploy
        #
        # resource_copy = copy.deepcopy(resource)
        #
        ret_code, message = fd_io_firestore.deploy_configuration(
            resource, firestore_project_id, uid)

        if ret_code is True:
            data["message"] = "Configuration deployed."
            http_status = 200
        else:
            return data["message"], 500

        # Do we need to deploy any associated CF
        #
        if deploy_cf is True:
            logging.info("GCP CF associated will be deployed if needed.")

            cf_deploy_flag, cf_resource = fd_io_gcp_cf.get_infos_for_cf_deployment(resource)

            if (cf_deploy_flag is False) and (cf_resource is None):
                data["message"] = "Error while parsing information for GCP CF deployment."
                return data, 400

            if (cf_deploy_flag is True) and (cf_resource is not None):
                ret_code, message = fd_io_gcp_cf.deploy_gcp_cf(
                    cf_resource, project_profile, uid)

                if ret_code is True:
                    data["message"] = "Configuration deployed AND Cloud Function deployed."
                    http_status = 200
                else:
                    data["message"] = message
                    return data, 500
            
            data["message"] = "Configuration deployed."
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
