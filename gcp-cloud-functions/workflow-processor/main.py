
from google.cloud import firestore
from google.cloud import exceptions
from google.cloud import pubsub_v1

import datetime
import dateutil.parser
import json
import uuid
from jinja2 import Template

import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account
import requests
import requests_toolbelt.adapters.appengine

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'


def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Figure out what environment we're running in and get some preliminary
    # information about the service account.
    bootstrap_credentials, _ = google.auth.default(
        scopes=[IAM_SCOPE])
    if isinstance(bootstrap_credentials,
                  google.oauth2.credentials.Credentials):
        raise Exception('make_iap_request is only supported for service '
                        'accounts.')
    elif isinstance(bootstrap_credentials,
                    google.auth.app_engine.Credentials):
        requests_toolbelt.adapters.appengine.monkeypatch()

    # For service account's using the Compute Engine metadata service,
    # service_account_email isn't available until refresh is called.
    bootstrap_credentials.refresh(Request())

    signer_email = bootstrap_credentials.service_account_email
    if isinstance(bootstrap_credentials,
                  google.auth.compute_engine.credentials.Credentials):
        # Since the Compute Engine metadata service doesn't expose the service
        # account key, we use the IAM signBlob API to sign instead.
        # In order for this to work:
        #
        # 1. Your VM needs the https://www.googleapis.com/auth/iam scope.
        #    You can specify this specific scope when creating a VM
        #    through the API or gcloud. When using Cloud Console,
        #    you'll need to specify the "full access to all Cloud APIs"
        #    scope. A VM's scopes can only be specified at creation time.
        #
        # 2. The VM's default service account needs the "Service Account Actor"
        #    role. This can be found under the "Project" category in Cloud
        #    Console, or roles/iam.serviceAccountActor in gcloud.
        signer = google.auth.iam.Signer(
            Request(), bootstrap_credentials, signer_email)
    else:
        # A Signer object can sign a JWT using the service account's key.
        signer = bootstrap_credentials.signer

    # Construct OAuth 2.0 service account credentials using the signer
    # and email acquired from the bootstrap credentials.
    service_account_credentials = google.oauth2.service_account.Credentials(
        signer, signer_email, token_uri=OAUTH_TOKEN_URI, additional_claims={
            'target_audience': client_id
        })

    # service_account_credentials gives us a JWT signed by the service
    # account. Next, we use that to obtain an OpenID Connect token,
    # which is a JWT signed by Google.
    google_open_id_connect_token = get_google_open_id_connect_token(
        service_account_credentials)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account {} does not have permission to '
                        'access the IAP-protected application.'.format(
                            signer_email))
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text


def get_google_open_id_connect_token(service_account_credentials):
    """Get an OpenID Connect token issued by Google for the service account.
    This function:
      1. Generates a JWT signed with the service account's private key
         containing a special "target_audience" claim.
      2. Sends it to the OAUTH_TOKEN_URI endpoint. Because the JWT in #1
         has a target_audience claim, that endpoint will respond with
         an OpenID Connect token for the service account -- in other words,
         a JWT signed by *Google*. The aud claim in this JWT will be
         set to the value from the target_audience claim in #1.
    For more information, see
    https://developers.google.com/identity/protocols/OAuth2ServiceAccount .
    The HTTP/REST example on that page describes the JWT structure and
    demonstrates how to call the token endpoint. (The example on that page
    shows how to get an OAuth2 access token; this code is using a
    modified version of it to get an OpenID Connect token.)
    """

    service_account_jwt = (
        service_account_credentials._make_authorization_grant_assertion())
    request = google.auth.transport.requests.Request()
    body = {
        'assertion': service_account_jwt,
        'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(
        request, OAUTH_TOKEN_URI, body)
    return token_response['id_token']


def update_workflow_status(doc, job_id, execution_date):


    # Get dict from doc
    #
    doc_dict = doc.to_dict()

    print(u'{} => {}'.format(doc.id, doc_dict))

    # Step 1 : retrieve current status based on doc.id
    #
    db = firestore.Client()
    current_status_ref = db.collection(u'workflow-status').document(doc.id)

    date_now = datetime.datetime.now().isoformat('T')

    current_status = current_status_ref.get().to_dict()
    if current_status is None:
        print("Status entry does not exists. Let's create it.")
        data = {"last_modified": date_now}
        current_status_ref.set(data)
        current_status = current_status_ref.get().to_dict()

    # Add job info
    #
    current_status['jobs'] = {
        job_id: {
            'executed': True,
            'execution_date': execution_date
        }
    }

    # Refresh "last_modified"
    #
    current_status['last_modified'] = date_now

    # refresh ACCOUNT and ENVIRONMENT
    #
    current_status['account'] = doc_dict["account"]
    current_status['environment'] = doc_dict["environment"]

    current_status_ref.set(current_status, merge=True)


def process_workflow_conditions(doc, job_id, execution_date):

    # Step 1 : retrieve current status based on doc.id
    #
    db = firestore.Client()
    current_status_ref = db.collection(u'workflow-status').document(doc.id)

    date_now = datetime.datetime.now().isoformat('T')

    current_status = current_status_ref.get().to_dict()
    if current_status is None:
        print("WORKFLOW STATUS not found : {}. Exiting ...".format(doc.id))
        return False

    doc_dict = doc.to_dict()

    # Check if all the JOBs have been executed
    #
    result = True
    for item in doc_dict['authorized_job_ids']:
        print(item.strip())

        try:
            if current_status['jobs'][item.strip()]['executed'] is False:
                result = False
        except KeyError:
            result = False
            break

    # Copy status to history and reset current status
    #
    if result is True:

        # Refresh Target DAH information
        #
        current_status['target_dag_last_executed'] = date_now
        current_status['target_dag'] = doc_dict['target_dag']

        # Copy current status to history
        #
        history_ref = db.collection(
            u'workflow-status').document(doc.id).collection('history').document(date_now)
        history_ref.set(current_status)

        # Reset current status and refresh target DAG info
        #
        for key in current_status['jobs'].keys():
            current_status['jobs'][key]['executed'] = False

        current_status['last_modified'] = date_now
        current_status_ref.set(current_status, merge=True)

    return result


def send_dag_trigger_to_pubsub( gcp_project_id, pubsub_topic, dag_id, dag_data):

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_project_id, pubsub_topic)

    payload = {}
    payload["dag_id"] = dag_id
    payload["dag_conf"] = dag_data["conf"]
    payload["dag_run_id"] = datetime.datetime.today().strftime("%Y%m%d-%H%M%S") + "-" + str(uuid.uuid4())

    data = bytes(json.dumps(payload), "utf-8")

    message_future = publisher.publish(topic_path, data=data)


def process(event, context):
    """Triggered by a change to a Firestore document.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    print("EVENT TYPE : " + context.event_type)
    print("RESOURCE   : " + context.resource)
    print("TIMESTAMP  : " + context.timestamp)

    # Let's check if we are not in a retry loop
    # If the event is older than an hour, we escape the loop
    #
    event_datetime = dateutil.parser.parse(context.timestamp)
    delta = datetime.datetime.now(datetime.timezone.utc) - event_datetime
    if delta >= datetime.timedelta(hours = 6):
        print("Event is too old. Exiting ...")
        return True

    
    document_id = (context.resource.strip()).rpartition("/")[2]
    collection_id = ((context.resource.strip()).rpartition("/")
                     [0]).rpartition("/")[2]


    print("COLLECTION_ID : " + collection_id)
    print("DOCUMENT_ID   : " + document_id)

    # Retrieve DAG ID
    #
    dag_id = None
    try:
        dag_id = event["value"]["fields"]["dag_id"]["stringValue"].strip()
        print("DAG ID : " + dag_id)
    except KeyError:
        print("Cannot find DAG ID. Exiting ...")
        return

    # Retrieve DAG RUN ID
    #
    dag_run_id = None
    try:
        dag_run_id = event["value"]["fields"]["dag_run_id"]["stringValue"].strip()
        print("DAG RUNf ID : " + dag_id)
    except KeyError:
        print("Cannot find DAG RUN ID.")
        dag_run_id = ""

    # Retrieve DAG EXECUTION DATE
    #
    dag_execution_date = None
    try:
        dag_execution_date = event["value"]["fields"]["dag_execution_date"]["stringValue"].strip()
        print("DAG EXECUTION date : " + dag_execution_date)
    except KeyError:
        print("Cannot find DAG EXECUTION DATE.")
        dag_execution_date = ""


    # Check for STATUS
    #
    status = None
    try:
        status = event["value"]["fields"]["status"]["stringValue"].strip()
        print("STATUS : " + status)
    except KeyError:
        print("Cannot find STATUS. Exiting ...")
        return

    # Exit if not SUCCESS
    #
    if status.strip() != "SUCCESS":
        print("Status is not \"SUCCESS\". Exiting...")
        return

    # Retrieve JOB ID
    #
    job_id = None
    try:
        job_id = event["value"]["fields"]["job_id"]["stringValue"].strip()
        print("JOB ID : " + job_id)
    except KeyError:
        print("Cannot find JOB ID. Exiting ...")
        return

    # Retrieve DAG EXECUTION DATE
    #
    execution_date = None
    try:
        execution_date = event["value"]["fields"]["dag_execution_date"]["stringValue"].strip(
        )
        print("EXECUTION DATE : " + execution_date)
    except KeyError:
        print("Cannot find EXECUTION DATE. Exiting ...")
        return

    # Retrieve Firestore documents mentioning that JOB ID
    #
    db = firestore.Client()

    # First, get the COMPOSER configuration
    #
    composer_configuration = (db.collection(
        u'gcp-cloud-functions').document(u'composer-configuration').get()).to_dict()

    # Get the workflow configuration data
    #
    docs = db.collection(u'workflow-configuration').where(
        u'authorized_job_ids', u'array_contains', job_id).get()

    # Process workflow-status with JOB ID info
    #
    for doc in docs:

        # Convert to dict
        #
        doc_dict = doc.to_dict()

        # Check if configuration is enabled or not
        #
        if doc_dict['activated'] is False:
            print("Configuration {} is disabled. Exiting ...".format(doc.id))
            continue

        # Parameters sent to the DAG
        #
        dag_parameters = {}
        dag_parameters['conf'] = {}

        # Process "passing parameters"
        # These parameters will be retrieved from the "triggering" DAG to the target DAG
        #
        # dag_id = event["value"]["fields"]["dag_id"]["stringValue"].strip()
        try:
            for item in doc_dict['passing_parameters']:
                print("passing_parameters : " + item)

                dag_parameters['conf'][item] = event["value"]["fields"][item]["stringValue"].strip(
                )

        except:
            print("No \"passing parameters\" to process")

        # Process Target DAG Parameters
        #
        try:
            for key in doc_dict['extra_parameters'].keys():
                dag_parameters['conf'][key] = doc_dict['extra_parameters'][key]
        except KeyError:
            print("No Target DAG parameters to process")

        # Update Status
        #
        update_workflow_status(doc, job_id, execution_date)

        # Get Composer/Airflow information
        #
        webserver_id = composer_configuration['webserver-id']
        client_id = composer_configuration['client-id']
        url_template = Template(composer_configuration['url'])
        url = url_template.render(
            WEBSERVER_ID=webserver_id, DAG_NAME=doc_dict['target_dag'].strip())

        # Process conditions
        #
        result = process_workflow_conditions(doc, job_id, execution_date)

        if result is True:
            print("Triggering DAG : " + doc_dict['target_dag'])

            # Send Order to pubsub
            #
            send_dag_trigger_to_pubsub( gcp_project_id=composer_configuration['gcp-project-id'],
                                        pubsub_topic=composer_configuration['pubsub-topic'],
                                        dag_id=doc_dict['target_dag'].strip(),
                                        dag_data=dag_parameters)

            # result = make_iap_request(url,
            #                           client_id,
            #                           method='POST',
            #                           data=json.dumps(dag_parameters))

            # print(result)

        else:
            print("Conditions are not met. Exiting...")
