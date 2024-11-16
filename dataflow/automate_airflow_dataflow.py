from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago

###################################################
BUCKET_NAME = 'earthquake-dataflow'
PROJECT_ID = 'delta-compass-440906'
LOCATION = 'us-central1'
###################################################
# DAG settings
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='daily_earthquake_ingestion_flow',
    default_args=default_args,
    description='Ingest earthquake data daily to Dataflow',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Define the Dataflow operator to run the Beam pipeline

    dataflow_operator = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_pipeline_daily',
        job_name='beam-dataflow-job',  # Unique job name
        py_file='gs://earthquake-project-files/dataflow_final/daily_data.py',  # Path to the pipeline on GCS
        location=LOCATION,
        gcp_conn_id='earth_conn',
        options={
            'project': PROJECT_ID,
            'region': LOCATION,
            'temp_location': f'gs://{BUCKET_NAME}/temp',
            'staging_location': f'gs://{BUCKET_NAME}/staging',  # Fixed typo: "stagging" → "staging"
            'runner': 'DataflowRunner',
            'input': f"gs://{BUCKET_NAME}/input/",
            'output': f"gs://{BUCKET_NAME}/output/",
        },
    )



