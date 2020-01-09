# -*- coding: utf-8 -*-

"""
    Fashiondata IO Credentials Module
    ========================================
 
    This module provides helper functions to handle credentials :
    
    - GCP Credentials
    - Firebase Admin Credentials

"""

import json
import logging

from google.cloud.storage import Client, Blob
from google.cloud import firestore
from google.oauth2 import service_account

# Globals
#
FIRESTORE_GCP_SA_INFOS_COLLECTION = "gcp-service-account"


def get_gcp_service_account_infos(gcp_project_id):

    try:
        # Instantiate Firestore Python Client with default credentials
        #
        db = firestore.Client()
        data = db.collection(FIRESTORE_GCP_SA_INFOS_COLLECTION).document(gcp_project_id).get().to_dict()
        return data["bucket"], data["blob"]

    except Exception as ex:
        logging.info("Error while retrieving GCP service account info from Firestore.")
        logging.info(ex)
        return None, None


def get_gcp_account(gcp_project_id):

    try:
        # Instantiate Firestore Python Client with default credentials
        #
        db = firestore.Client()
        data = db.collection(FIRESTORE_GCP_SA_INFOS_COLLECTION).document(gcp_project_id).get().to_dict()
        return data["account"]

    except Exception as ex:
        logging.info("Error while retrieving GCP account from Firestore.")
        logging.info(ex)
        return None, None


def get_gcp_service_account_credentials(gcp_project_id):

    # Retrieve service account information corresponding to the GCP Project ID provided
    #
    bucket, blob_name = get_gcp_service_account_infos(gcp_project_id)

    if (bucket is None) or (blob_name is None):
        return None
    
    try:
        # Read the credentials from GCS
        #
        gcs_client = Client()
        bucket = gcs_client.get_bucket(bucket)
        blob = Blob(blob_name, bucket)
        json_credentials = json.loads(blob.download_as_string())

        # Build and return GCP Credentials
        #
        return service_account.Credentials.from_service_account_info(json_credentials)

    except Exception as ex:
        print("Cannot retrieve Service Account credentials.") 
        print(ex)
        return None
