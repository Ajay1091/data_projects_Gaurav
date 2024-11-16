###########################################################################################################
"""
file_name = final_historical.py
desc = manual_data ingection by using datflow in bigquary
date = 2024/11/10
version = 2

"""
############################################################################################################


## importing libraries
from google.cloud import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.gcsio import GcsIO
import pyarrow  #as pa
#import pyarrow.parquet as pq
import io
import json
import os
import requests
import pyarrow
import logging
from datetime import datetime
import re

############# setup some comman variables #############

## date str
date_str = datetime.now().strftime('%Y%m%d') + 'daily'

## google service account key
GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\earthquake\key\delta-compass-440906-dca1fe6ea297.json'

## url for monthly basis
URL_MONTH = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'

### GCP storage Bucket variables
PROJECT_NAME = 'delta-compass-440906'
BUCKET_NAME = 'earthquake-dataflow'

# Configure Google Cloud options
JOB_NAME_HISTORICAL = f'historical{date_str}'
JOB_NAME_DAILY = f'daily{date_str}'
BQ_ANALYSIS = 'analysis_with'
LOCATION = 'us-central1'
STAGGING = f'gs://{BUCKET_NAME}/stagging'
TEMP = f'gs://{BUCKET_NAME}/temp'

## raw file gcs path
RAW_LAYER_PATH = f'gs://{BUCKET_NAME}/raw_layer_final/daily{date_str}'
RAW_LAYER_PATH_READ = f'gs://{BUCKET_NAME}/raw_layer_final/daily{date_str}.json'

### parquate schema
CLEAN_DATA_PARQUTE_SCHEMA = pyarrow.schema([
    ('mag', pyarrow.float64()),
    ('place', pyarrow.string()),
    ('time', pyarrow.string()),  # Timestamp in seconds
    ('updated', pyarrow.string()),  # Timestamp in seconds
    ('tz', pyarrow.string()),
    ('url', pyarrow.string()),
    ('detail', pyarrow.string()),
    ('felt', pyarrow.float64()),
    ('cdi', pyarrow.float64()),
    ('mmi', pyarrow.float64()),
    ('alert', pyarrow.string()),
    ('status', pyarrow.string()),
    ('tsunami', pyarrow.int64()),
    ('sig', pyarrow.int64()),
    ('net', pyarrow.string()),
    ('code', pyarrow.string()),
    ('ids', pyarrow.string()),
    ('sources', pyarrow.string()),
    ('types', pyarrow.string()),
    ('nst', pyarrow.int64()),
    ('dmin', pyarrow.float64()),
    ('rms', pyarrow.float64()),
    ('gap', pyarrow.int64()),
    ('magType', pyarrow.string()),
    ('type', pyarrow.string()),
    ('title', pyarrow.string()),
    ('area', pyarrow.string()),
    ('longitude', pyarrow.float64()),
    ('latitude', pyarrow.float64()),
    ('depth', pyarrow.float64()),
    ('insert_date', pyarrow.string())  # Timestamp in seconds
])

# for silver layer path
WRITE_PARQUATE = f'gs://{BUCKET_NAME}/silver_layer_final/daily{date_str}'

## bigquary variables
PROJECT_ID = 'delta-compass-440906'
DATASET_NAME = 'earthquake_project'
TABLE_NAME = 'dataflow_eartheqake_final'
STAGING_BUCKET = 'earthquake-project-files'
TABLE = f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}'
AUDIT_NAME = 'dataflow_audit_log'
AUDIT_TABLE = f'{PROJECT_ID}.{DATASET_NAME}.{AUDIT_NAME}'

########################################################################################################

############## common function used for transformation #################################################

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def audit_event(job_id, pipeline_name, function_name, start_time, end_time, status, error_msg):
    client = bigquery.Client()
    table_id = AUDIT_TABLE # Replace with actual project and dataset details

    # Convert datetime objects to string format
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_time, datetime) else start_time
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_time, datetime) else end_time

    rows_to_insert = [
        {
            "job_id": job_id,
            "pipeline_name": pipeline_name,
            "function_name": function_name,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "status": status,
            "error_type": error_msg

        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("Errors occurred while inserting audit record:", errors)
    else:
        print("Audit record inserted successfully")



### read data from api call
class ReadDataFromApiJson:

    @staticmethod
    def reading(url):
        """
        url : to fecth data from Api
        """
        """Fetch data from the API and log audit events."""
        start_time = datetime.utcnow()  # Start time for auditing
        try:
            response = requests.get(url)
            end_time = datetime.utcnow()  # End time for auditing
            if response.status_code == 200:
                content = response.json()
                logging.info(f"Data successfully retrieved from {url}")
                audit_event(date_str, "EarthquakePipeline", "reading", start_time, end_time, "Success", None)  # Log success
                return content
            else:
                logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
                audit_event(date_str, "EarthquakePipeline", "reading", start_time, end_time, "Failed", f"Status code: {response.status_code}")  # Log failure
                return None
        except requests.exceptions.RequestException as e:
            end_time = datetime.utcnow()  # End time for auditing
            logging.error(f"Error occurred while retrieving data from {url}: {e}")
            audit_event(date_str, "EarthquakePipeline", "reading", start_time, end_time, "Failed", str(e))  # Log failure
            return None


# Function to flatten feature data
def feature_flatten(element):
    try:
        content = json.loads(element)
        features = content['features']
        data = []
        for value in features:
            properties = value['properties']
            geometry = {
                        'longitude': value['geometry']['coordinates'][0],
                        'latitude': value['geometry']['coordinates'][1],
                        'depth': value['geometry']['coordinates'][2]
                    }
            properties.update(geometry)
            data.append(properties)
        return data
    except Exception as e:
        logging.error(f"Error flattening features: {e}")
        return []

## data transformation function
def transformation(element):
    for values in element:
        try:
            # Convert 'time' and 'updated' to UTC format if possible
            time = float(values.get('time', 0)) / 1000
            values['time'] = datetime.utcfromtimestamp(time).strftime(
                '%Y-%m-%d %H:%M:%S') if time > 0 else None

            update = float(values.get('updated', 0)) / 1000
            values['updated'] = datetime.utcfromtimestamp(update).strftime(
                '%Y-%m-%d %H:%M:%S') if update > 0 else None

            # Extract 'area' from 'place'
            place = values.get('place', '')
            match = re.search(r"of\s+(.+)$", place)
            values['area'] = match.group(1).strip() if match else None

            # Set 'insert_date' to current UTC time
            values['insert_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            # Prepare the earthquake dictionary
            earthquake_dic = {
                "mag": values.get("mag"),
                "place": values.get("place"),
                "time": values.get("time"),
                "updated": values.get("updated"),
                "tz": values.get("tz"),
                "url": values.get("url"),
                "detail": values.get("detail"),
                "felt": values.get("felt"),
                "cdi": values.get("cdi"),
                "mmi": values.get("mmi"),
                "alert": values.get("alert"),
                "status": values.get("status"),
                "tsunami": values.get("tsunami"),
                "sig": values.get("sig"),
                "net": values.get("net"),
                "code": values.get("code"),
                "ids": values.get("ids"),
                "sources": values.get("sources"),
                "types": values.get("types"),
                "nst": values.get("nst"),
                "dmin": values.get("dmin"),
                "rms": values.get("rms"),
                "gap": values.get("gap"),
                "magType": values.get("magType"),
                "type": values.get("type"),
                "title": values.get("title"),
                "area": values.get("area"),
                "longitude": values.get("longitude"),
                "latitude": values.get("latitude"),
                "depth": values.get("depth"),
                "insert_date": values.get("insert_date")
            }

            yield earthquake_dic
        except Exception as e:
            logging.error(f"Error processing element: {e}")


######################################################################################################


################ main programm for with dataflow approch ####################################

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Start of the outer try block
if __name__ == "__main__":
    try:
        start_time_outer = datetime.utcnow()  # Start time for entire pipeline

        # Set environment variable for Google credentials
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
        options = PipelineOptions()

        # Configure Google Cloud options
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = PROJECT_NAME
        google_cloud_options.job_name = JOB_NAME_HISTORICAL
        google_cloud_options.region = LOCATION
        google_cloud_options.staging_location = STAGGING
        google_cloud_options.temp_location = TEMP

        # Step 1: Data read from API
        try:
            start_time = datetime.utcnow()
            content = ReadDataFromApiJson.reading(URL_MONTH)
            content_as_string = json.dumps(content)
            end_time = datetime.utcnow()
            logger.info("Data successfully retrieved from API.")
            audit_event(date_str, "EarthquakePipeline", "Data Retrieval from API", start_time, end_time, "Success", None)
        except Exception as e:
            end_time = datetime.utcnow()
            logger.error(f"Error retrieving data from API: {e}")
            audit_event(date_str, "EarthquakePipeline", "Data Retrieval from API", start_time, end_time, "Failed", str(e))
            raise

        # Step 2: Load JSON data to GCS bucket
        try:
            start_time = datetime.utcnow()
            with beam.Pipeline(options=options) as p1:
                file_GCS = (
                    p1
                    | 'Create PCollection' >> beam.Create([content_as_string])
                    | 'Write to GCS' >> beam.io.WriteToText(
                        RAW_LAYER_PATH, file_name_suffix='.json', shard_name_template='', num_shards=1
                    )
                )
            end_time = datetime.utcnow()
            logger.info("JSON data written to GCS successfully.")
            audit_event(date_str, "EarthquakePipeline", "Write JSON to GCS", start_time, end_time, "Success", None)
        except Exception as e:
            end_time = datetime.utcnow()
            logger.error(f"Error writing JSON data to GCS: {e}")
            audit_event(date_str, "EarthquakePipeline", "Write JSON to GCS", start_time, end_time, "Failed", str(e))
            raise

        # Step 3: Flatten, transform, and write data to GCS in Parquet format and BigQuery
        try:
            with beam.Pipeline(options=options) as p2:
                # Step 3a: Read and flatten raw data from GCS
                try:
                    start_time = datetime.utcnow()
                    flatten_data = (
                        p2
                        | 'Read Raw Data' >> beam.io.ReadFromText(RAW_LAYER_PATH_READ)
                        | 'Flatten Features' >> beam.Map(feature_flatten)
                    )
                    end_time = datetime.utcnow()
                    logger.info("Data flattened successfully.")
                    audit_event(date_str, "EarthquakePipeline", "Flatten Data", start_time, end_time, "Success", None)
                except Exception as e:
                    end_time = datetime.utcnow()
                    logger.error(f"Error flattening data: {e}")
                    audit_event(date_str, "EarthquakePipeline", "Flatten Data", start_time, end_time, "Failed", str(e))
                    raise

                # Step 3b: Transform data
                try:
                    start_time = datetime.utcnow()
                    transform_data = (
                        flatten_data
                        | 'Transform Data' >> beam.FlatMap(transformation)
                    )
                    end_time = datetime.utcnow()
                    logger.info("Data transformation completed successfully.")
                    audit_event(date_str, "EarthquakePipeline", "Transform Data", start_time, end_time, "Success", None)
                except Exception as e:
                    end_time = datetime.utcnow()
                    logger.error(f"Error transforming data: {e}")
                    audit_event(date_str, "EarthquakePipeline", "Transform Data", start_time, end_time, "Failed", str(e))
                    raise

                # Step 3c: Write transformed data to GCS as Parquet
                try:
                    start_time = datetime.utcnow()
                    parqute_silver = (
                        transform_data
                        | 'parqute to GCS' >> beam.io.WriteToParquet(
                        WRITE_PARQUATE, schema=CLEAN_DATA_PARQUTE_SCHEMA, file_name_suffix='.parquet',
                        shard_name_template=''
                        )
                    )
                    end_time = datetime.utcnow()
                    logger.info("Data written to GCS as Parquet successfully.")
                    audit_event(date_str, "EarthquakePipeline", "Write Parquet to GCS", start_time, end_time, "Success", None)
                except Exception as e:
                    end_time = datetime.utcnow()
                    logger.error(f"Error writing Parquet to GCS: {e}")
                    audit_event(date_str, "EarthquakePipeline", "Write Parquet to GCS", start_time, end_time, "Failed", str(e))
                    raise

                # Step 3d: Write transformed data to BigQuery
                try:
                    start_time = datetime.utcnow()

                    bq_write =  (transform_data
                     | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                                table=TABLE,
                                schema='SCHEMA_AUTODETECT',
                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                     )
                    end_time = datetime.utcnow()
                    logger.info("Data written to BigQuery successfully.")
                    audit_event(date_str, "EarthquakePipeline", "Write to BigQuery", start_time, end_time, "Success", None)
                except Exception as e:
                    end_time = datetime.utcnow()
                    logger.error(f"Error writing to BigQuery: {e}")
                    audit_event(date_str, "EarthquakePipeline", "Write to BigQuery", start_time, end_time, "Failed", str(e))
                    raise

        except Exception as e:
            logger.error(f"Pipeline transformation and loading failed: {e}")
            raise

        # Outer try block audit event for overall success
        end_time_outer = datetime.utcnow()
        audit_event(date_str, "EarthquakePipeline", "Overall Pipeline Execution", start_time_outer, end_time_outer, "Success", None)

    except Exception as e:
        end_time_outer = datetime.utcnow()
        logger.error(f"Pipeline failed: {e}")
        audit_event(date_str, "EarthquakePipeline", "Overall Pipeline Execution", start_time_outer, end_time_outer, "Failed", str(e))