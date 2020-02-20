# -*- coding: utf-8 -*-

import os
import argparse
import datetime
import json
import base64
import warnings
import pprint
import logging

from google.cloud import storage
import google.cloud.exceptions

import google.oauth2.credentials
import googleapiclient.discovery

from fd_io_python_libs.fd_io_credentials import fd_io_credentials
from fd_io_python_libs.fd_io_firebase import fd_io_firebase_admin_sdk
from fd_io_python_libs.fd_io_users import fd_io_users


def upload_to_gcs_from_file(dag_data, dag_filename, gcp_project_id, gcp_composer_bucket, uid):

    logging.info("Deploying DAG file to Composer bucket , under GCP Project : %s", gcp_project_id)

    # Route to proper configuration deployment processor
    #
    try:

        # Get user email
        #
        user_email = fd_io_firebase_admin_sdk.get_firebase_user_email(uid)
        logging.info("Deploy configuration, email used : {}".format(user_email))

        # Check user permission
        #
        permissions_to_check = [fd_io_users.GCP_WRITER]
        if fd_io_users.gcp_check_permission(user_email, gcp_project_id, "storage", permissions_to_check) is False:
            return False, "Not enough permission to write configuration to GC Storage."
        
        # Get credentials
        #
        sa_credentials = fd_io_credentials.get_gcp_service_account_credentials(gcp_project_id)
        if sa_credentials is None :
            logging.info("Error while retting SA credentials.")
            return False, "Error while handling SA credentials."

        # Write DAG data to GCS
        #
        client = storage.Client(project=gcp_project_id, credentials=sa_credentials)
        bucket = client.get_bucket(gcp_composer_bucket)
        blob = storage.Blob(dag_filename, bucket)
        blob.upload_from_string(dag_data)

        return True, "DAG file deployed successfully to Bucket."

    except Exception as ex:
        message = "Error while parsing resource : %s" % ex
        print(message)
        return False, message


def check_file_exists(dag_filename_full_path, gcp_project_id, gcp_composer_bucket):

    logging.info("Checking for DAG file existence : {}/{}".format(gcp_composer_bucket, dag_filename_full_path))

    # Route to proper configuration deployment processor
    #
    try:
        
        # Get credentials
        #
        sa_credentials = fd_io_credentials.get_gcp_service_account_credentials(gcp_project_id)
        if sa_credentials is None :
            logging.info("Error while retting SA credentials.")
            return False

        # Write DAG data to GCS
        #
        client = storage.Client(project=gcp_project_id, credentials=sa_credentials)
        bucket = client.get_bucket(gcp_composer_bucket)

        return isinstance(bucket.get_blob(dag_filename_full_path), storage.Blob)

    except Exception as ex:
        logging.info("Error while checking for DAG file existence : {}/{}".format(gcp_composer_bucket, dag_filename_full_path))
        logging.info(ex)
        return False