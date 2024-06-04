from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/isaachall97@gmail.com/pinterest-data-pipeline81/mount_s3_to_databricks',
}

# Define params for Run Now Operator
notebook_params = {
    "Variable": 5
}

default_args = {
    'owner': '0affee876ba9',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('0affee876ba9_dag',
         start_date=datetime(2024, 5, 23),  
         schedule_interval='@daily',  # runs at midnight every day
         catchup=False,
         default_args=default_args
         ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )

# Ensure the task is added to the DAG
opr_submit_run

