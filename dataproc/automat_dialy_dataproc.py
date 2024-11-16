from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator, DataprocSubmitJobOperator, DataprocStopClusterOperator
from airflow.utils.dates import days_ago

#################################
PROJECT_ID = 'delta-compass-440906'
LOCATION = 'us-central1'
CLUSTER_NAME = 'cluster-6f43'
#################################
# DAG settings
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='daily_earthquake_ingestion_dataproc',
    default_args=default_args,
    description='Daily ingestion of earthquake data into BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=LOCATION,
        cluster_name=CLUSTER_NAME,
    )

    dataproc_job = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': 'gs://earthquake-project-files/dataproc_final/dialy_data.py',
            'python_file_uris': [
                'gs://earthquake-project-files/dataproc_final/configuration.py',
                'gs://earthquake-project-files/dataproc_final/utility.py',
            ],
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar'],
        },
    }

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id='submit_daily_dataproc_job',
        job=dataproc_job,
        region=LOCATION,
        project_id=PROJECT_ID,
        gcp_conn_id='earth_conn',
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",  # Changed task_id to make it unique
        project_id=PROJECT_ID,
        region=LOCATION,
        cluster_name=CLUSTER_NAME,
    )

    start_cluster >> submit_dataproc_job >> stop_cluster
