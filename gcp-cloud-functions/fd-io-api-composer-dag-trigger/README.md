# Fashiondata IO : Composer DAG Trigger

**Description**

This Cloud Function allow external apps to trigger a DAG within a specific GCP Project.

This CF will use PubSub to do so, so the account executing the function must have the right permissions to publish on the specific PubSub topic.

The following Environment variables must be declared upon deployement.

- PS_COMPOSER_DAG_TRIGGER_GCP_PROJECT_ID : GCP Project ID of the PubSub and Composer instances to be requested.
- PS_COMPOSER_DAG_TRIGGER_TOPIC : PubSub Topic where the DAG trigger request will be sent.
- FB_ADMIN_SDK_BUCKET : GCP Storage bucket where the Firebase credentials are located.
- FB_ADMIN_SDK_CREDENTIALS_PATH : path to the Firebase credentials within the bucket.
