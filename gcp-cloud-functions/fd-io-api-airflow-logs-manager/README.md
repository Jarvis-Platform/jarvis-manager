# Fashiondata IO : Airflow / Composer Logs Manager

**Description**

This Cloud Function allow external apps to retrieve Airflow / Composer Logs.

This function is protected by Firebase authentication.

The following Environment variables must be declared upon deployement.

- FB_ADMIN_SDK_BUCKET : GCP Storage bucket where the Firebase credentials are located.
- FB_ADMIN_SDK_CREDENTIALS_PATH : path to the Firebase credentials within the bucket.