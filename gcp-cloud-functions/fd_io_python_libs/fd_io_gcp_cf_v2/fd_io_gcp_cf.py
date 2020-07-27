# -*- coding: utf-8 -*-

import os
import io
import argparse
import datetime
import time
import json
import base64
import warnings
import pprint
import logging
import platform
import subprocess
import paramiko

from google.cloud import firestore
import google.cloud.exceptions
from google.cloud.storage import Client, Blob

import google.oauth2.credentials
import googleapiclient.discovery

from fd_io_python_libs.fd_io_credentials import fd_io_credentials
from fd_io_python_libs.fd_io_firebase import fd_io_firebase_admin_sdk
from fd_io_python_libs.fd_io_users import fd_io_users
from fd_io_python_libs.fd_io_firestore_v2 import fd_io_firestore


# Globals
#

# Worker IP : replace with loaded configuration
#
WORKER_IP = "34.67.231.54"

LOCAL_DIR_BUCKET_FUSE = "/home/ubuntu/mnt/"
LOCAL_DIR_CREDENTIALS = LOCAL_DIR_BUCKET_FUSE + "credentials/"

GCLOUD_COMMAND = "/snap/bin/gcloud"

SOURCE_REPOSITORY_PREFIX = "https://source.developers.google.com/projects/fd-jarvis-datalake/repos/fd-jarvis-datalake-platform/moveable-aliases/master/paths/"

JARVIS_DEPLOY_GCP_CF_PREFIX = "jarvis [--gcp-project-id PROJECT-ID] deploy gcp-cloud-function"

SUPPORTED_CONFIGURATION_TYPES = ["storage-to-storage", "storage-to-table", "storage-to-tables", "vm-launcher"]

GCP_CF_TYPES = {
    "gcs-to-storage": {
        "num_arguments": 5,
        "help": JARVIS_DEPLOY_GCP_CF_PREFIX + " gcs-to-storage CLOUD_FUNCTION_NAME GCS_BUCKET_WATCHED DIRECTORY_FILTER CONFIGURATION_ID",
        "command": GCLOUD_COMMAND + """ functions deploy {CLOUD_FUNCTION_NAME} \
--source """ + SOURCE_REPOSITORY_PREFIX + """cloud-functions/gcs-to-storage \
--region=europe-west1 --entry-point=process --retry --runtime=python37 --allow-unauthenticated \
--trigger-event=google.storage.object.finalize --trigger-resource={BUCKET_NAME} \
--set-env-vars DIRECTORY_FILTER={DIRECTORY_FILTER} \
--set-env-vars CONFIGURATION_ID={CONFIGURATION_ID} \
--set-env-vars FIRESTORE_PROJECT_ID={FIRESTORE_PROJECT_ID} \
--set-env-vars PROJECT_ID={GCP_PROJECT_ID} --project={GCP_PROJECT_ID} --account={GCP_ACCOUNT}
"""
    },
    "gcs-to-gcs": {
        "num_arguments": 4,
        "help": JARVIS_DEPLOY_GCP_CF_PREFIX + " gcs-to-gcs CLOUD_FUNCTION_NAME BUCKET_NAME DIRECTORY_FILTER",
        "command": GCLOUD_COMMAND + """ functions deploy {CLOUD_FUNCTION_NAME} \
--source """ + SOURCE_REPOSITORY_PREFIX + """cloud-functions/gcs-to-gcs \
--region=europe-west1 --entry-point=triggerDag --retry --runtime=nodejs10 --allow-unauthenticated \
--trigger-event=google.storage.object.finalize --trigger-resource={BUCKET_NAME} \
--set-env-vars DIRECTORY_FILTER={DIRECTORY_FILTER} \
--set-env-vars FIRESTORE_PROJECT_ID={FIRESTORE_PROJECT_ID} \
--set-env-vars PROJECT_ID={GCP_PROJECT_ID} --project={GCP_PROJECT_ID} --account={GCP_ACCOUNT}"""
    },
    "gcs-to-gbq": {
        "num_arguments": 4,
        "help": JARVIS_DEPLOY_GCP_CF_PREFIX + " gcs-to-gbq CLOUD_FUNCTION_NAME BUCKET_NAME DIRECTORY_FILTER",
        "command": GCLOUD_COMMAND + """ functions deploy {CLOUD_FUNCTION_NAME} \
--source """ + SOURCE_REPOSITORY_PREFIX + """cloud-functions/gcs-to-gbq \
--region=europe-west1 --entry-point=triggerDag --retry --runtime=nodejs10 --allow-unauthenticated \
--trigger-event=google.storage.object.finalize --trigger-resource={BUCKET_NAME} \
--set-env-vars DIRECTORY_FILTER={DIRECTORY_FILTER} \
--set-env-vars FIRESTORE_PROJECT_ID={FIRESTORE_PROJECT_ID} \
--set-env-vars PROJECT_ID={GCP_PROJECT_ID} --project={GCP_PROJECT_ID} --account={GCP_ACCOUNT}"""
    },
    "storage-to-dag": {
        "num_arguments": 6,
        "help": JARVIS_DEPLOY_GCP_CF_PREFIX + " storage-to-dag CLOUD_FUNCTION_NAME BUCKET_NAME DIRECTORY_FILTER DAG_TO_TRIGGER CONFIGURATION_ID",
        "command": GCLOUD_COMMAND + """ functions deploy {CLOUD_FUNCTION_NAME} \
--source """ + SOURCE_REPOSITORY_PREFIX + """cloud-functions/storage-to-dag \
--region=europe-west1 --entry-point=process_send_trigger_to_pubsub --retry --runtime=python37 --allow-unauthenticated \
--trigger-event=google.storage.object.finalize --trigger-resource={BUCKET_NAME} \
--set-env-vars DIRECTORY_FILTER={DIRECTORY_FILTER} \
--set-env-vars DAG_TO_TRIGGER={DAG_TO_TRIGGER} \
--set-env-vars CONFIGURATION_ID={CONFIGURATION_ID} \
--set-env-vars COMPOSER_DAG_TRIGGER_PUBSUB_GCP_PROJECT={COMPOSER_DAG_TRIGGER_PUBSUB_GCP_PROJECT} \
--set-env-vars COMPOSER_DAG_TRIGGER_PUBSUB_TOPIC={COMPOSER_DAG_TRIGGER_PUBSUB_TOPIC} \
--set-env-vars PROJECT_ID={GCP_PROJECT_ID} --project={GCP_PROJECT_ID} --account={GCP_ACCOUNT}"""
    },
    "storage-to-storage-cf-edition": {
        "num_arguments": 6,
        "help": JARVIS_DEPLOY_GCP_CF_PREFIX + " storage-to-storage-cf-edition CLOUD_FUNCTION_NAME BUCKET_NAME DIRECTORY_FILTER DAG_TO_TRIGGER CONFIGURATION_ID",
        "command": GCLOUD_COMMAND + """ functions deploy {CLOUD_FUNCTION_NAME} \
--source """ + SOURCE_REPOSITORY_PREFIX + """cloud-functions/storage-to-storage-cf-edition \
--region=europe-west1 --entry-point=process --retry --runtime=python37 --allow-unauthenticated --memory=1024 --timeout=540 \
--trigger-event=google.storage.object.finalize --trigger-resource={BUCKET_NAME} \
--set-env-vars DIRECTORY_FILTER={DIRECTORY_FILTER} \
--set-env-vars DAG_TO_TRIGGER={DAG_TO_TRIGGER} \
--set-env-vars CONFIGURATION_ID={CONFIGURATION_ID} \
--set-env-vars FIRESTORE_PROJECT_ID={FIRESTORE_PROJECT_ID} \
--set-env-vars PUBSUB_PROJECT_ID={PUBSUB_PROJECT_ID} \
--max-instances={CF_MAX_INSTANCES} \
--set-env-vars PROJECT_ID={GCP_PROJECT_ID} --project={GCP_PROJECT_ID} --account={GCP_ACCOUNT}"""
    },
    "storage-to-tables-cf-edition": {
        "num_arguments": 6,
        "help": JARVIS_DEPLOY_GCP_CF_PREFIX + " storage-to-tables-cf-edition CLOUD_FUNCTION_NAME BUCKET_NAME DIRECTORY_FILTER DAG_TO_TRIGGER CONFIGURATION_ID",
        "command": GCLOUD_COMMAND + """ functions deploy {CLOUD_FUNCTION_NAME} \
--source """ + SOURCE_REPOSITORY_PREFIX + """cloud-functions/storage-to-tables-cf-edition \
--region=europe-west1 --entry-point=process --retry --runtime=python37 --allow-unauthenticated --timeout=540 \
--trigger-event=google.storage.object.finalize --trigger-resource={BUCKET_NAME} \
--set-env-vars DIRECTORY_FILTER={DIRECTORY_FILTER} \
--set-env-vars DAG_TO_TRIGGER={DAG_TO_TRIGGER} \
--set-env-vars CONFIGURATION_ID={CONFIGURATION_ID} \
--set-env-vars FIRESTORE_PROJECT_ID={FIRESTORE_PROJECT_ID} \
--set-env-vars PUBSUB_PROJECT_ID={PUBSUB_PROJECT_ID} \
--max-instances={CF_MAX_INSTANCES} \
--set-env-vars PROJECT_ID={GCP_PROJECT_ID} --project={GCP_PROJECT_ID} --account={GCP_ACCOUNT}"""
    }
}


def get_infos_for_cf_deployment(configuration):
    """
        Parse configuration and returns an array of arguments prepared for a GCP cf deployment

        :param configuration: associated configuration
        :type configuration: map
        :return: a non-empty array containing the arguments. None otherwise
        :rtype: array[str]

    """

    try:

        # Check if the configuration type is supported
        #
        logging.info("Trying to process configuration type : {}".format(configuration["configuration_type"]))
        if configuration["configuration_type"] not in SUPPORTED_CONFIGURATION_TYPES:
            logging.info("Configuration type \"{}\" is not supported.".format(configuration["configuration_type"]))
            logging.info("DEBUG")
            return True, None

        # Handling type : storage-to-storage
        #
        if configuration["configuration_type"] in ["storage-to-storage", "storage-to-tables"]:

            # Checking source
            #
            logging.info("Trying to process \"source\" : {}".format(configuration["source"]["type"]))

            if configuration["source"]["type"] == "gcs":

                cf_suffix = (configuration["source"]["gcs_source_prefix"].rstrip("/")).replace("/", "-")

                # Process Cloud Function name
                #
                cf_name = "UNKNOWN"
                if configuration["configuration_type"] == "storage-to-storage":
                    cf_name = "mirror" + "-" + configuration["source"]["gcs_source_bucket"] + "-" + cf_suffix
                elif configuration["configuration_type"] == "storage-to-tables":
                    
                    # CF Name cannot begin with a digit
                    #
                    if (configuration["source"]["gcs_source_bucket"])[0].isdigit() is True:
                        cf_name = "cf-" + configuration["source"]["gcs_source_bucket"] + "-" + cf_suffix + "-to-bigquery"
                    else:
                        cf_name = configuration["source"]["gcs_source_bucket"] + "-" + cf_suffix + "-to-bigquery"

                # TODO
                # Hack for the STS Cloud Function edition
                #
                if configuration["configuration_type"] == "storage-to-storage":
                    if len(configuration["destinations"]) >= 1:
                        if (configuration["destinations"][0])["type"] == "gcs":

                            logging.info("STS CF EDITION HACK")

                            payload = {
                                "cf_type" : "storage-to-storage-cf-edition",
                                "cf_name" : cf_name,
                                "bucket_name" : configuration["source"]["gcs_source_bucket"],
                                "directory_filter" : configuration["source"]["gcs_source_prefix"].rstrip("/"),
                                "configuration_type" : configuration["configuration_type"],
                                "configuration_id" : configuration["configuration_id"]
                            }

                            # Retrieve optional "CF Max Instances" from configuration
                            #
                            try:
                                payload["cf_max_instances"] = configuration["max_active_runs"]
                            except Exception:
                                payload["cf_max_instances"] = 5

                            return True, payload

                # Hack for the STT Cloud Function edition
                #
                elif configuration["configuration_type"] == "storage-to-tables":
                    if len(configuration["destinations"]) >= 1:
                        if (configuration["destinations"][0])["type"] == "bigquery":

                            logging.info("STT CF EDITION HACK")

                            payload = {
                                "cf_type" : "storage-to-tables-cf-edition",
                                "cf_name" : cf_name,
                                "bucket_name" : configuration["source"]["gcs_source_bucket"],
                                "directory_filter" : configuration["source"]["gcs_source_prefix"].rstrip("/"),
                                "configuration_type" : configuration["configuration_type"],
                                "configuration_id" : configuration["configuration_id"]
                            }

                            # Retrieve optional "CF Max Instances" from configuration
                            #
                            try:
                                payload["cf_max_instances"] = configuration["max_active_runs"]
                            except Exception:
                                payload["cf_max_instances"] = 1

                            return True, payload

                # Normal path
                #
                logging.info("Deploy through generic path ...")
                return True, [
                    "storage-to-dag",
                    cf_name,
                    configuration["source"]["gcs_source_bucket"],
                    configuration["source"]["gcs_source_prefix"].rstrip("/"),
                    configuration["configuration_type"],
                    configuration["configuration_id"]
                ]

            else:

                return True, None

        # Handling type : storage-to-table
        #
        if configuration["configuration_type"] == "storage-to-table":

            cf_suffix = (configuration["gcs_prefix"].rstrip("/")).replace("/", "-")

            # CF Name cannot begin with a digit
            #
            if (configuration["gcs_bucket"])[0].isdigit() is True:
                cf_name = "cf-" + configuration["gcs_bucket"] + "-" + cf_suffix + "-process-to-bigquery"
            else:
                cf_name = configuration["gcs_bucket"] + "-" + cf_suffix + "-process-to-bigquery"

            return True, [
                    "gcs-to-gbq",
                    cf_name,
                    configuration["gcs_bucket"],
                    configuration["gcs_prefix"].rstrip("/")
            ]

        # Handling VM Launcher
        #
        # Will work for PGP only
        #
        elif configuration["configuration_type"] == "vm-launcher":
        
            logging.info("Special processing vm-launcher type deployment...")

            if "pgp_mode" in configuration.keys():

                cf_name = "cf-" + configuration["configuration_id"]

                return True, [
                        "storage-to-dag",
                        cf_name,
                        configuration["gcs_source_bucket"],
                        configuration["gcs_source_prefix"].rstrip("/"),
                        "storage-to-pgp",
                        configuration["configuration_id"]
                    ]
            
            logging.info("This VM LAUNCHER configuration is not supported.")

    except Exception as ex:
        logging.info(
            "Error while parsing configuration file for CF deployment.\n{}".format(ex))
        return False, None

    return False, None


def get_ssh_client(ipaddress):

    # Instantiate SSH Client
    # Global timeout : 60 seconds
    #
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    date_start = datetime.datetime.now()
    while True:

        # global Timeout check
        if (datetime.datetime.now()-date_start).total_seconds() > 20:
            return None

        try:
            private_key = None
            # read private key from GCS
            #
            gcs_client = Client()
            bucket = gcs_client.get_bucket("fd-io-jarvis-platform-api")
            blob = Blob("credentials/fd-io-key.private", bucket)
            read_key = str(blob.download_as_string(), "utf-8")

            with io.StringIO() as f:
                f.write(read_key)
                f.seek(0)
                private_key = paramiko.rsakey.RSAKey.from_private_key(f)

            client.connect(hostname=ipaddress, timeout=10,
                           look_for_keys=False, pkey=private_key, passphrase='', username='ubuntu')

            # If we are here this means we connected successfully to the instance
            #
            break

        except paramiko.ssh_exception.NoValidConnectionsError as novalid:
            logging.info(novalid)
            logging.info(
                "Error during SSH connection to the instance. Retrying ...")
        except Exception as ex:
            logging.info(ex)
            logging.info(
                "Error during SSH connection to the instance. Retrying ...")

        time.sleep(5)
    # End WHILE

    return client


def build_command_deploy_gcp_cf(resource=None,
                                gcp_project_id=None,
                                gcp_account=None,
                                firestore_project_id=None,
                                composer_dag_trigger_pubsub_gcp_project=None,
                                composer_dag_trigger_pubsub_topic=None):

    if resource is not None:

        if len(resource) >= 1:

            try:
                # TODO
                # New approach
                #
                try:
                    cf_type = resource["cf_type"]

                    if cf_type in ["storage-to-storage-cf-edition"]:
                        command = (GCP_CF_TYPES[cf_type]["command"]).format(CLOUD_FUNCTION_NAME=resource["cf_name"],
                                                                            BUCKET_NAME=resource["bucket_name"],
                                                                            DIRECTORY_FILTER=resource["directory_filter"],
                                                                            DAG_TO_TRIGGER=resource["configuration_type"],
                                                                            CONFIGURATION_ID=resource["configuration_id"],
                                                                            CF_MAX_INSTANCES=resource["cf_max_instances"],
                                                                            FIRESTORE_PROJECT_ID=firestore_project_id,
                                                                            PUBSUB_PROJECT_ID=composer_dag_trigger_pubsub_gcp_project,
                                                                            GCP_ACCOUNT=gcp_account,
                                                                            GCP_PROJECT_ID=gcp_project_id)
                    elif cf_type in ["storage-to-tables-cf-edition"]:
                        command = (GCP_CF_TYPES[cf_type]["command"]).format(CLOUD_FUNCTION_NAME=resource["cf_name"],
                                                                            BUCKET_NAME=resource["bucket_name"],
                                                                            DIRECTORY_FILTER=resource["directory_filter"],
                                                                            DAG_TO_TRIGGER=resource["configuration_type"],
                                                                            CONFIGURATION_ID=resource["configuration_id"],
                                                                            CF_MAX_INSTANCES=resource["cf_max_instances"],
                                                                            FIRESTORE_PROJECT_ID=firestore_project_id,
                                                                            PUBSUB_PROJECT_ID=composer_dag_trigger_pubsub_gcp_project,
                                                                            GCP_ACCOUNT=gcp_account,
                                                                            GCP_PROJECT_ID=gcp_project_id)

                    else:

                        # GCP CF type unknown
                        #
                        return None, "GCP CF type \"{}\" unknown.".format(resource[0])

                    return command, "ok"

                except Exception:
                    logging.info("Falling back to OLD fashion deployment ...")
                
                # Check if we have a known GCP CF type
                #
                if resource[0] not in GCP_CF_TYPES.keys():
                    message = "GCP Cloud Function type \"{}\" unknown.".format(
                        resource[0])
                    logging.info(message)
                    return None, message

                # Check if we have the right number of arguments
                #
                if GCP_CF_TYPES[resource[0]]["num_arguments"] != len(resource):
                    message = "Please provide the proper amount of arguments. There are {}, there should be {}.\n{}\n\nusage : {}".format(
                        str(len(resource)), str(GCP_CF_TYPES[resource[0]]["num_arguments"]), resource, GCP_CF_TYPES[resource[0]]["help"])
                    logging.info(message)
                    return None, message


                # TODO
                # Old way
                #
                # Assign values to the command according to its type
                #
                if resource[0] in ["gcs-to-gcs", "gcs-to-gbq"]:
                    command = (GCP_CF_TYPES[resource[0]]["command"]).format(CLOUD_FUNCTION_NAME=resource[1],
                                                                            BUCKET_NAME=resource[2],
                                                                            DIRECTORY_FILTER=resource[3],
                                                                            FIRESTORE_PROJECT_ID=firestore_project_id,
                                                                            GCP_ACCOUNT=gcp_account,
                                                                            GCP_PROJECT_ID=gcp_project_id)
                elif resource[0] in ["storage-to-dag"]:
                    command = (GCP_CF_TYPES[resource[0]]["command"]).format(CLOUD_FUNCTION_NAME=resource[1],
                                                                            BUCKET_NAME=resource[2],
                                                                            DIRECTORY_FILTER=resource[3],
                                                                            DAG_TO_TRIGGER=resource[4],
                                                                            CONFIGURATION_ID=resource[5],
                                                                            COMPOSER_DAG_TRIGGER_PUBSUB_GCP_PROJECT=composer_dag_trigger_pubsub_gcp_project,
                                                                            COMPOSER_DAG_TRIGGER_PUBSUB_TOPIC=composer_dag_trigger_pubsub_topic,
                                                                            GCP_ACCOUNT=gcp_account,
                                                                            GCP_PROJECT_ID=gcp_project_id)
                elif resource[0] in ["gcs-to-storage"]:
                    command = (GCP_CF_TYPES[resource[0]]["command"]).format(CLOUD_FUNCTION_NAME=resource[1],
                                                                            BUCKET_NAME=resource[2],
                                                                            DIRECTORY_FILTER=resource[3],
                                                                            CONFIGURATION_ID=resource[4],
                                                                            FIRESTORE_PROJECT_ID=firestore_project_id,
                                                                            GCP_ACCOUNT=gcp_account,
                                                                            GCP_PROJECT_ID=gcp_project_id)

                else:

                    # GCP CF type unknown
                    #
                    return None, "GCP CF type \"{}\" unknown.".format(resource[0])

                return command, "ok"

            except KeyError as ex:
                message = "Error while processing GCP CF type : %s" % ex
                logging.info(message)
                return None, message
            except Exception as ex:
                message = "Global Error while processing GCP CF type : %s" % ex
                logging.info(message)
                return None, message
        else:
            message = "\"resource\" argument is empty."
            return None, message
    else:
        message = "\"resource\" argument is missing."
        return None, message


def deploy_gcp_cf(resource, project_profile, uid):

    # Get GCP Project ID from project profile
    #
    gcp_project_id = fd_io_firestore.get_gcp_project_id_from_project_profile(
        project_profile)

    # Get user email
    #
    user_email = fd_io_firebase_admin_sdk.get_firebase_user_email(uid)

    # Check user permission
    #
    permissions_to_check = [fd_io_users.GCP_WRITER]
    if fd_io_users.gcp_check_permission(user_email, gcp_project_id, "gcp-cloud-function", permissions_to_check) is False:
        message = "Not enough permission to write configuration to deploy GCP Cloud Functions."
        logging.info(message)
        return False, message

    # Get credentials information
    #
    cred_bucket, cred_blob = fd_io_credentials.get_gcp_service_account_infos(
        gcp_project_id)
    if (cred_bucket is None) or (cred_blob is None):
        message = "Error while retrieving SA information. Check if the proper credentials have been deployed on server side."
        logging.info(message)
        return False, message

    # Get Account
    #
    gcp_account = fd_io_credentials.get_gcp_account(gcp_project_id)
    if gcp_account is None:
        message = "Error while retrieving GCP Account."
        logging.info(message)
        return False, message

    logging.info("GCP Account retrieved : {}".format(gcp_account))

    # Get Firestore Project ID from project profile
    #
    firestore_project_id = fd_io_firestore.get_firestore_project_id_from_project_profile(
        project_profile)

    # Get Composer DAG Trigger info
    #
    composer_dag_trigger_pubsub_gcp_project = fd_io_firestore.get_composer_dag_trigger_pubsub_gcp_project_from_project_profile(project_profile)
    composer_dag_trigger_pubsub_topic = fd_io_firestore.get_composer_dag_trigger_pubsub_topic_from_project_profile(project_profile)


    # Build gcloud deploy command according to the "cf type" which should be the first argument
    # passed in the resource item
    #
    # resource=None, gcp_project_id=None, gcp_account=None, firestore_project_id=None
    deploy_gcp_cf_command, message = build_command_deploy_gcp_cf(
                                        resource=resource,
                                        gcp_project_id=gcp_project_id,
                                        gcp_account=gcp_account,
                                        firestore_project_id=firestore_project_id,
                                        composer_dag_trigger_pubsub_gcp_project=composer_dag_trigger_pubsub_gcp_project,
                                        composer_dag_trigger_pubsub_topic=composer_dag_trigger_pubsub_topic)

    if deploy_gcp_cf_command is None:
        return False, message

    # Get SSH client
    #
    ssh_client = get_ssh_client(WORKER_IP)
    if ssh_client is None:
        message = "Error while instantiating SSH client."
        logging.info(message)
        return False, message

    logging.info(
        "Deploying GCP Cloud Function under GCP Project : %s", gcp_project_id)

    # Execute list off all commands
    #
    shell_error = False
    error_message = ""
    stdin = None
    stdout = None
    stderr = None

    # Authenticate SA command
    #
    auth_sa_command = GCLOUD_COMMAND + \
        """ auth activate-service-account --key-file=""" + \
        LOCAL_DIR_BUCKET_FUSE + cred_blob

    commands = [
        auth_sa_command,
        deploy_gcp_cf_command
    ]

    for command in commands:

        command += """
if [ $? -ne 0 ]; then
    echo "ERROR_WHILE_EXECUTING_COMMAND";
fi
"""

        try:
            print("Executing command : %s" % command)
            stdin, stdout, stderr = ssh_client.exec_command(command)
            print("Command executed.")

            shell_error = False
            stdout_content = str(stdout.read(), "utf-8")
            for line in stdout_content.splitlines():
                print(line)
                if "ERROR_WHILE_EXECUTING_COMMAND" in line:
                    print("Error while executing command.")
                    shell_error = True

            if shell_error is True:
                print("Shell error happened ...")
                error_buffer = None
                error_buffer = str(stderr.read(), "utf-8")
                print("Error buffer length : %s", str(len(error_buffer)))
                if len(error_buffer) > 0:
                    for line in error_buffer.splitlines():
                        error_message += line
                break

        except paramiko.ssh_exception.SSHException as ex:
            message = 'SSH exceptionError while executing command : %s' % ex
            return False, message
        except Exception as ex:
            message = 'Unknown exception while executing command : %s' % ex
            return False, message

    # Close properly the SSH client
    #
    print("Closing SSH Client ...")
    ssh_client.close()

    # Send if there is an error
    #
    if shell_error is True:
        return False, error_message

    return True, "Cloud Function deployed successfully."
