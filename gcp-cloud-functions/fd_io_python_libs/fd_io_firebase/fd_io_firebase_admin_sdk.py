# -*- coding: utf-8 -*-

"""
    Fashiondata IO Firebase Admin SDK Module
    ========================================
 
    This module is a simple interface to Firebase Admin SDK module.
    This also provides some helper function to handle simple permission requests.
 
    Subtitle
    -------------------
 
    You can say so many things here ! You can say so many things here !

"""

import json
import logging

from google.cloud.storage import Client, Blob

import firebase_admin
from firebase_admin import auth
from firebase_admin import credentials


# GLOBALS
#
FB_ADMIN_SDK_BUCKET = "fd-io-jarvis-platform-api"
FB_ADMIN_SDK_CREDENTIALS_PATH = "credentials/fd-io-jarvis-platform-firebase-adminsdk.json"

# PERMISSIONS
#
ADMINISTRATOR = "administrator"
WRITER = "writer"
VIEWER = "viewer"
DESIGNER = "designer"


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


def get_firebase_user_email(uid):

    try:
        default_app = get_firebase_app()
        user = auth.get_user(uid, app=default_app)
        return user.email

    except Exception as ex:
        print("Error while retrieving user's email.")
        print(ex)
        return None


def get_firebase_user_claims(uid):

    try:
        default_app = get_firebase_app()

        user = auth.get_user(uid, app=default_app)

        logging.info("User email : %s", user.email)

        return user.custom_claims.get('gcp_permissions')

    except Exception as ex:
        print("Error while instantiating Firebase Admin DEFAULT APP.")
        print(ex)
        return None


def get_firebase_user_accounts(uid):

    try:
        default_app = get_firebase_app()

        user = auth.get_user(uid, app=default_app)

        logging.info("User email : %s", user.email)

        return user.custom_claims.get('customer_accounts')

    except Exception as ex:
        print("Error while attempting to retrieve CUSTOMER ACCOUNTS.")
        print(ex)
        return None


def gcp_check_permission(uid, gcp_project_id, resource, permission):
    """
        Check user's permission for a GCP resource within a specific project.
 
        :param uid: The user's Firebase UID
        :type uid: str
        :param gcp_project_id: Google Cloud Platform project ID associated with the resource
        :type gcp_project_id: str
        :param resource: The resource to check permissions against, i.e : firestore, storage, bigquery, ...
        :type resource: str
        :param permission: The permission level asked
        :type permission: str
        :return: True if user has write permission to the resource, False otherwise
        :rtype: bool 

    """
    
    firebase_user_claims = get_firebase_user_claims(uid)
    if firebase_user_claims is None:
        raise Exception("Firebase user/claims not found.")

    try:

        # logging.info(firebase_user_claims)

        for project in firebase_user_claims.keys():
            if gcp_project_id == project:
                for uc_resource in firebase_user_claims[project].keys():
                    if resource == uc_resource:
                        if firebase_user_claims[project][uc_resource] in [ADMINISTRATOR, permission]:
                            return True
        
        # No match
        #
        return False

    except Exception as ex:
        logging.error("Error while parsing user's claim.")
        raise ex