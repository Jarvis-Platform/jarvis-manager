# -*- coding: utf-8 -*-

"""
    Fashiondata IO USERS Module
    ========================================
 
    This module provides helper functions to handle USERS :
    
    - GCP Credentials
    - Firebase Admin Credentials
    - Permissions
    - ...

"""

import json
import logging

from google.cloud import firestore


# Globals
#
FIRESTORE_USER_PERMISSIONS_COLLECTION_ID = "user-permissions"
FIRESTORE_USER_PERMISSIONS_GCP_ATTRIBUTE_ID = "gcp_permissions"

# PERMISSIONS : GCP projects
#
GCP_ADMINISTRATOR = "administrator"
GCP_WRITER = "writer"
GCP_VIEWER = "viewer"
GCP_USER = "user"


def gcp_check_permission(user_email, gcp_project_id, resource, permissions_to_check):
    """
            Check user's permission for a GCP resource within a specific project.
    
            :param user_email: The user's email
            :type user_email: str
            :param gcp_project_id: Google Cloud Platform project ID associated with the resource
            :type gcp_project_id: str
            :param resource: The resource to check permissions against, i.e : firestore, storage, bigquery, ...
            :type resource: str
            :param permissions_to_check: The permissions to check against
            :type permissions_to_check: array[str]
            :return: True if user has write permission to the resource, False otherwise
            :type: bool 

    """

    try:
        db = firestore.Client()
        gcp_permissions = (db.collection(FIRESTORE_USER_PERMISSIONS_COLLECTION_ID).document(user_email).get()).to_dict()

        # Administrator level has all permissions, so we can safely add it to the list of permissions to check
        #
        permissions_to_check.append(GCP_ADMINISTRATOR)

        if gcp_permissions[FIRESTORE_USER_PERMISSIONS_GCP_ATTRIBUTE_ID][gcp_project_id][resource] in permissions_to_check:
            return True
        
        return False

    except Exception as ex:
        logging.info("Error while retrieving user permission from Firestore for email : {}\n{}".format(user_email, ex))
        return False