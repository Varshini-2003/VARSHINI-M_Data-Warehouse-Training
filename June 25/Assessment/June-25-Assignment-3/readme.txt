Go to Airflow UI:

Trigger the parent DAG parent_trigger

Wait a few seconds

Open the child DAG child_target

Check log_trigger_metadata logs you’ll see:

Child DAG triggered by: parent_trigger
Run ID: manual__...
Execution Date: ...
Note: Triggered from parent DAG