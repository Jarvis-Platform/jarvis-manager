# -*- coding: utf-8 -*-

import os
import argparse
import datetime
import json
import base64
import warnings
import pprint
import logging

from google.cloud import firestore
import google.cloud.exceptions

import google.oauth2.credentials
import googleapiclient.discovery

from fd_io_python_libs.fd_io_credentials import fd_io_credentials
from fd_io_python_libs.fd_io_firebase import fd_io_firebase_admin_sdk
from fd_io_python_libs.fd_io_users import fd_io_users

# Global
#
_configuration_type_mapping_ = {
    "storage-to-storage" : "storage-to-storage-conf",
    "workflow" : "workflow-configuration",
    "vm-launcher" : "vm-launcher-conf",
    "gbq-to-gcs" : "gbq-to-gcs-conf",
    "storage-to-table" : "mirror-exc-gcs-to-gbq-conf",
    "storage-to-tables" : "storage-to-tables-conf",
    "gcs-to-gcs" : "mirror-exc-gcs-to-gcs-conf",
    "table-to-storage" : "table-to-storage-conf",
    "dag-configuration" : "dag-configuration",
    "gbq-to-gbq" : "gbq-to-gbq-conf"
}


def special_processing_table_to_storage(resource):

    resource["sql"] = bytes(resource["sql"], "utf-8")
    print(resource["sql"])
    return resource


def get_user_info():

    oauth2_client = googleapiclient.discovery.build(
        'oauth2', 'v2')

    return oauth2_client.userinfo().get().execute()


def get_project_profile_from_id(project_profile):

    # Instantiate Firestore Python Client
    #
    db = firestore.Client()

    # Get profile with given ID
    #
    try:
        return (db.collection(u'project-profile').document(project_profile.strip()).get()).to_dict()

    except Exception as ex:
        print("Error while retrieving Project Profiles with ID : {}\n{}".format(project_profile.strip(), ex))
        return None


def get_gcp_project_id_from_project_profile(project_profile):

    # Retrieve project profile
    #
    profile = get_project_profile_from_id(project_profile)

    if profile is None:
        return None

    # Parse the profile to get the GCP Project ID
    #
    try:
        return profile["gcp_project_id"]

    except KeyError as ex:
        print("Cannot retrieve GCP Project ID from profile : {}".format(project_profile))
        return None


def get_composer_gcp_project_id_from_project_profile(project_profile):

    # Retrieve project profile
    #
    profile = get_project_profile_from_id(project_profile)

    if profile is None:
        return None

    # Parse the profile to get the COMPOSER GCP Project ID
    #
    try:
        return profile["composer_project_id"]

    except KeyError as ex:
        print("Cannot retrieve Composer Instance GCP Project ID from profile : {}".format(project_profile))
        return None


def get_firestore_project_id_from_project_profile(project_profile):

    # Retrieve project profile
    #
    profile = get_project_profile_from_id(project_profile)

    if profile is None:
        return None

    # Parse the profile to get the Firestore Project ID
    #
    try:
        return profile["firestore_project_id"]

    except KeyError as ex:
        print("Cannot retrieve Firestore Project ID from profile : {}".format(project_profile))
        return None


def get_composer_bucket_from_project_profile(project_profile):

    # Retrieve project profile
    #
    profile = get_project_profile_from_id(project_profile)

    if profile is None:
        return None

    # Parse the profile to get the Composer Bucket
    #
    try:
        return profile["composer_bucket"]

    except KeyError as ex:
        print("Cannot retrieve Composer Bucket from profile : {}".format(project_profile))
        return None


def get_project_profile(accounts):

    # Instantiate Firestore Python Client
    #
    db = firestore.Client()

    # Get profiles
    #
    try:
        global_list = []
        for account in accounts:
            doc_list = db.collection(u'project-profile').where(u'account', u'==' , account)
            for doc in doc_list.stream():
                global_list.append(doc.id)

        return global_list

    except Exception as ex:
        print("Error while retrieving Project Profiles : {}".format(ex))
        return None


def get_project_profile_from_gcp_project_id(gcp_project_id):

    # Instantiate Firestore Python Client
    #
    db = firestore.Client()

    # Get profiles
    #
    try:

        for doc in (db.collection(u'project-profile').where(u'gcp_project_id', u'==' , gcp_project_id)).stream():
            return doc.id

        return None

    except Exception as ex:
        print("Error while retrieving Project Profiles from GCP Project ID \"{}\" : {}".format(gcp_project_id, ex))
        return None


def get_document(collection, document):

    # Instantiate Firestore Python Client
    #
    db = firestore.Client()

    # Get profiles
    #
    try:
        return (db.collection(collection).document(document).get()).to_dict()

    except Exception as ex:
        print("Error while retrieving Firestor document {} in collection {}.\n{}".format(document, collection, ex))
        return None


def update_infos_processing(data, doc, user_infos):

    date_now = datetime.datetime.now().isoformat('T')

    if doc is not None:

        print("Configuration exists. Updating ...")
        
        # Check if created_by exists
        #
        try:
            tmp = doc["created_by"]

            data["created_by"] = doc["created_by"]
            data["creation_date"] = doc["creation_date"]

        except KeyError:
            
            # Does not exists we add it
            #
            print("Super update ...")
            data["created_by"] = user_infos["email"].strip()
            data["creation_date"] = date_now

        # Add update info
        #
        data["update_date"] = date_now
        data["updated_by"] = user_infos["email"].strip()

    else:

        print("Configuration does not exists. Creation in progress ...")

        # Document does not exist
        #
        data["created_by"] = user_infos["email"].strip()
        data["creation_date"] = date_now
        data["update_date"] = date_now
        data["updated_by"] = user_infos["email"].strip()

    return data


def deploy_regular_configuration(resource, gcp_project_id, user_credentials, user_email, update_time):

    # Instantiate Firestore Python Client
    #
    db = firestore.Client(project=gcp_project_id, credentials=user_credentials)

    collection_id = None
    configuration_id = None
    try:

        # Retrieve the collection ID according to the collection type
        collection_id = _configuration_type_mapping_[resource["configuration_type"]]

    except KeyError as ex:

        message = "Error while parsing configuration headers %s : " % ex
        print(message)
        return False, message

    # Tag the payload with user email and update time
    #
    resource["updated_by"] = user_email
    resource["update_date"] = update_time

    # Special processing
    #
    try:

        sub_collection = None
        sub_document_id = None

        if resource["configuration_type"] in ["gbq-to-gcs", "table-to-storage"]:
            resource = special_processing_table_to_storage(resource)
            configuration_id = resource["configuration_id"].strip()

        elif resource["configuration_type"] == "storage-to-table":

            logging.info("deploy_regular_configuration : STT")

            sub_collection = "CONFIGURATION"
            sub_document_id = resource["filename_template"]
            configuration_id = resource["gcs_bucket"]

            # Specific for GCS to GBQ  jobs
            # We add "account", "environment" and "gcp_project" to first document level
            #
            data = {"account": resource["account"], 
                    "environment": resource["environment"],
                    "gcp_project": resource["gcp_project"]}

            db.collection(collection_id).document(configuration_id).set(data, merge=True)

        elif resource["configuration_type"] == "storage-to-storage":

            if resource["source"]["type"] == "gcs":
                resource["source"]["gcp_project_id"] = gcp_project_id
                configuration_id = resource["configuration_id"].strip()
        else:
            
            # Common case for configuration_id
            #
            configuration_id = resource["configuration_id"].strip()


    except KeyError as ex:
        message = "Error while parsing configuration information : {} ".format(ex)
        print(message)
        return False, message

    # Write the configuration
    #
    logging.info("Writing to Firestore ...")
    try:
        if (sub_collection is not None) and (sub_document_id is not None):
            db.collection(collection_id).document(configuration_id).collection(sub_collection).document(sub_document_id).set(resource)
        else:

            # Check if there is a current configuration
            #
            current_conf = db.collection(collection_id).document(configuration_id).get()

            if current_conf.exists is True:

                # Archive current configuration
                #
                archive_collection_id = collection_id + "-archive"
                archive_document_id = configuration_id
                archive_sub_collection_id = "history"
                archive_sub_document_id = update_time

                db.collection(archive_collection_id).document(archive_document_id).collection(archive_sub_collection_id).document(archive_sub_document_id).set(current_conf.to_dict())

            # Writing
            #
            db.collection(collection_id).document(configuration_id).set(resource)

        return True, "Success"

    except Exception as ex:
        logging.info(ex)
        return False, ex


def deploy_configuration(resource, gcp_project_id, uid):

    logging.info("Deploying configuration to Firestore, under GCP Project : %s", gcp_project_id)
    # logging.info(resource)

    # Route to proper configuration deployment processor
    #
    try:

        # Get config type
        #
        config_type = resource["configuration_type"]
        logging.info("Configuration type : {}".format(config_type))

        # Get user email
        #
        user_email = fd_io_firebase_admin_sdk.get_firebase_user_email(uid)
        logging.info("Deploy configuration, email used : {}".format(user_email))

        # Check user permission
        #
        permissions_to_check = [fd_io_users.GCP_WRITER]
        if fd_io_users.gcp_check_permission(user_email, gcp_project_id, "firestore", permissions_to_check) is False:
            return False, "Not enough permission to write configuration to Firestore."
        
        # Get credentials
        #
        sa_credentials = fd_io_credentials.get_gcp_service_account_credentials(gcp_project_id)
        if sa_credentials is None :
            logging.info("Error while retting SA credentials.")
            return False, "Error while handling SA credentials."

        # Get time
        #
        date_now = datetime.datetime.now().isoformat('T')

        # Route
        #
        if config_type in _configuration_type_mapping_.keys():
            return deploy_regular_configuration(resource, gcp_project_id, sa_credentials, user_email, date_now) 
        else:
            message = "Configuration Type unknown : %s" % config_type
            print(message)
            return False, message

    except KeyError as ex:
        message = "Error while parsing resource : %s" % ex
        print(message)
        return False, message
