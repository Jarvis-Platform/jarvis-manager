# Workflow Processor

**Description**

The purpose of this cloud function is to offer a simple workflow processing to orchestrate DAGs executions.

This GCP Cloud Function is triggered upon any changes in Firestore and filters DAG runs.

If a DAG run successfully, this function will search through Firestore any "workflow" type configurations that matches the DAG's job ID and process the associated workflow to eventually trigger another process (DAG).
