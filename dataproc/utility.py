##########################################################################################
"""
project_name = earthquake-project-dataproc
file_name = utility.py
date - 2024-10-21
desc - common functions and class
version - 1
date of modification - 2024/11/15
"""

###########################################################################################




from google.cloud import bigquery
import configuration as conf
from pyspark.sql import SparkSession
import requests
import os
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType,FloatType
from datetime import datetime
import json
import pyspark.sql.functions as f
import logging

## google crediential
# os.environ[
#     'GOOGLE_APPLICATION_CREDENTIALS'] = conf.GOOGLE_APPLICATION_CREDENTIALS

date_str = datetime.now().strftime('%Y%m%d') + 'daily'

def audit_event(job_id, pipeline_name, function_name, start_time, end_time, status, error_msg, process_record):
    client = bigquery.Client()
    table_id = f"{conf.PROJECT_ID}.{conf.DATASET_NAME}.{conf.AUDIT_TABLE}" # Replace with actual project and dataset details

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
            "error_type": error_msg,
            "number_of_record": process_record
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("Errors occurred while inserting audit record:", errors)
    else:
        print("Audit record inserted successfully")




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
                audit_event(date_str, "EarthquakePipeline", "reading", start_time, end_time, "Success", None, 1)  # Log success
                return content
            else:
                logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
                audit_event(date_str, "EarthquakePipeline", "reading", start_time, end_time, "Failed", f"Status code: {response.status_code}", 0)  # Log failure
                return None
        except requests.exceptions.RequestException as e:
            end_time = datetime.utcnow()  # End time for auditing
            logging.error(f"Error occurred while retrieving data from {url}: {e}")
            audit_event(date_str, "EarthquakePipeline", "reading", start_time, end_time, "Failed", str(e), 0)  # Log failure
            return None


class UploadtoGCS:
    @staticmethod
    def uploadjson(bucket_name, data_object,destination_blob_name):

        """
        Uploads a JSON object to a GCS bucket and logs the event to BigQuery.
        :param bucket_name: bucket name where we want to store data
        :param data_object: the json object
        :return: None
        """
        start_time = datetime.utcnow()
        try:
            # Initialize GCS client and bucket
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Create a unique file name with the current date
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"{date_str}.json"

            # Specify the blob (path) within the bucket where the file will be stored

            blob = bucket.blob(destination_blob_name)

            # Upload the JSON data directly from memory to GCS
            blob.upload_from_string(data=json.dumps(data_object), content_type='application/json')

            end_time = datetime.utcnow()
            audit_event(date_str, "EarthquakePipeline", "uploadjson", start_time, end_time, "Success", None, 1)
            logging.info(f"Upload of {filename} to {destination_blob_name} complete.")
        except Exception as e:
            end_time = datetime.utcnow()
            audit_event(date_str, "EarthquakePipeline", "uploadjson", start_time, end_time, "Failed", str(e), 0)
            logging.error(f"Error uploading JSON to GCS: {e}")


class GCSFileDownload:
    def __init__(self, bucket_name):
        """
        :param bucket_name: bucket name where your json file is stored
        """
        """Initialize the class with bucket information."""
        self.storage_client = storage.Client()  # Uses the environment variable for credentials
        self.bucket_name = bucket_name

    def download_json_as_text(self, json_file_path):
        """
        :param json_file_path: file path where the json file is stored
        :return: json in dictionary form
        """
        """Download a JSON file from GCS and return it as a Python dictionary."""
        start_time = datetime.utcnow()  # Log the start time
        try:
            # Access the GCS bucket
            bucket = self.storage_client.bucket(self.bucket_name)

            # Retrieve the JSON file from the bucket
            blob = bucket.blob(json_file_path)

            # Download the JSON data as a text string
            json_data = blob.download_as_text()

            # Convert the JSON string to a Python dictionary
            json_dict = json.loads(json_data)

            end_time = datetime.utcnow()  # Log the end time

            # Log the audit event for successful download
            audit_event(date_str, "EarthquakePipeline", "download_json_as_text", start_time, end_time, "Success", None, 1)

            logging.info(f"Successfully downloaded {json_file_path} from GCS.")

            # Return the JSON data as a dictionary
            return json_dict
        except Exception as e:
            end_time = datetime.utcnow()  # Log the end time on failure

            # Log the audit event for failed download
            audit_event(date_str, "EarthquakePipeline", "download_json_as_text", start_time, end_time, "Failed", str(e), 0)

            logging.error(f"Error downloading {json_file_path} from GCS: {e}")
            return None  # Return None in case of error


class EarthquakeDataFrameCreation:
    def __init__(self, json_data):
        """
        :param json_data: data from GCS bucket in JSON form
        """
        self.json_data = json_data
        self.spark = SparkSession.builder.appName('Earthquake Data Processor').getOrCreate()

        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def process_data(self):
        """
        :return: processed raw data
        """
        start_time = datetime.utcnow()  # Start time for auditing
        try:
            features = self.json_data['features']
            self.logger.info('Processing %d features', len(features))
            data = []
            for value in features:
                properties = value['properties']
                # Extract geometry coordinates
                properties['geometry'] = {
                    'longitude': value['geometry']['coordinates'][0],
                    'latitude': value['geometry']['coordinates'][1],
                    'depth': value['geometry']['coordinates'][2]
                }
                data.append(properties)

            end_time = datetime.utcnow()  # End time for auditing
            audit_event(date_str, "EarthquakePipeline", "process_data", start_time, end_time,
                        "Success", None, len(data))
            return data
        except KeyError as e:
            end_time = datetime.utcnow()
            self.logger.error('KeyError: %s', e)
            audit_event(date_str, "EarthquakePipeline", "process_data", start_time, end_time,
                        "Failed", str(e), 0)
        except Exception as e:
            end_time = datetime.utcnow()
            self.logger.error('An error occurred while processing data: %s', e)
            audit_event(date_str, "EarthquakePipeline", "process_data", start_time, end_time,
                        "Failed", str(e), 0)

    def convert_to_dataframe(self):
        """
        :return: final DataFrame
        """
        start_time = datetime.utcnow()  # Start time for auditing
        try:
            data = self.process_data()

            schema = StructType([
                StructField("mag", FloatType(), True),
                StructField("place", StringType(), True),
                StructField("time", StringType(), True),
                StructField("updated", StringType(), True),
                StructField("tz", StringType(), True),
                StructField("url", StringType(), True),
                StructField("detail", StringType(), True),
                StructField("felt", IntegerType(), True),
                StructField("cdi", FloatType(), True),
                StructField("mmi", FloatType(), True),
                StructField("alert", StringType(), True),
                StructField("status", StringType(), True),
                StructField("tsunami", IntegerType(), True),
                StructField("sig", IntegerType(), True),
                StructField("net", StringType(), True),
                StructField("code", StringType(), True),
                StructField("ids", StringType(), True),
                StructField("sources", StringType(), True),
                StructField("types", StringType(), True),
                StructField("nst", IntegerType(), True),
                StructField("dmin", FloatType(), True),
                StructField("rms", FloatType(), True),
                StructField("gap", FloatType(), True),
                StructField("magType", StringType(), True),
                StructField("type", StringType(), True),
                StructField("title", StringType(), True),
                StructField("longitude", FloatType(), True),
                StructField("latitude", FloatType(), True),
                StructField("depth", FloatType(), True)
            ])

            processed_data = []
            for entry in data:
                processed_entry = {}
                for key, value in entry.items():
                    # Convert fields to float where necessary
                    if key in ['mag', 'cdi', 'mmi', 'dmin', 'rms', 'gap','depth']:
                        processed_entry[key] = float(value) if value is not None else None
                    else:
                        processed_entry[key] = value
                processed_data.append(processed_entry)

            # Create Spark DataFrame
            df = self.spark.createDataFrame(processed_data, schema)
            self.logger.info('DataFrame created successfully with %d records', df.count())

            end_time = datetime.utcnow()  # End time for auditing
            audit_event(date_str, "EarthquakePipeline", "convert_to_dataframe", start_time,
                        end_time, "Success", None, df.count())
            return df
        except Exception as e:
            end_time = datetime.utcnow()
            self.logger.error('An error occurred while converting data to DataFrame: %s', e)
            audit_event(date_str, "EarthquakePipeline", "convert_to_dataframe", start_time,
                        end_time, "Failed", str(e), 0)


class Transformation:
    @staticmethod
    def process(df):
        """
        :return: transformed DataFrame
        """
        start_time = datetime.utcnow()  # Start time for auditing
        try:
            # Transformations
            df_transformed = df \
                .withColumn('of_position', f.instr(f.col('place'), 'of')) \
                .withColumn('length', f.length(f.col('place'))) \
                .withColumn('area', f.expr("substring(place, of_position + 3, length - of_position - 2)")) \
                .withColumn('time', f.from_unixtime(f.col('time') / 1000)) \
                .withColumn('updated', f.from_unixtime(f.col('updated') / 1000)) \
                .withColumn('insert_date', f.current_timestamp()) \
                .drop('of_position', 'length')

            record_count = df_transformed.count()  # Get the number of records in transformed DataFrame
            end_time = datetime.utcnow()  # End time for auditing
            audit_event(date_str, "EarthquakePipeline", "transform_data", start_time, end_time,
                        "Success", None, record_count)

            return df_transformed
        except Exception as e:
            end_time = datetime.utcnow()  # End time for auditing in case of failure
            error_message = f'An error occurred during DataFrame transformation: {e}'
            audit_event(date_str, "EarthquakePipeline", "transform_data", start_time, end_time,
                        "Failed", error_message, 0)
            raise RuntimeError(error_message)


class SilverParquet:
    @staticmethod
    def upload_parquet(df, GCS_path):
        """
        Uploads the DataFrame to a Parquet file in Google Cloud Storage.

        :param df: DataFrame to upload to GCS as a Parquet file
        :param GCS_path: The GCS path (gs://your-bucket/path/to/output/)
        :return: None
        """
        start_time = datetime.utcnow()  # Start time for auditing
        try:
            # Write the DataFrame to Parquet
            df.write.parquet(GCS_path)
            record_count = df.count()  # Number of records processed
            end_time = datetime.utcnow()  # End time for auditing

            # Log success
            audit_event(date_str, "EarthquakePipeline", "upload_parquet", start_time, end_time,
                        "Success", None, record_count)
            logging.info(f"Successfully uploaded DataFrame to {GCS_path}")
        except Exception as e:
            end_time = datetime.utcnow()  # End time for auditing in case of failure
            error_message = f"Error uploading DataFrame: {e}"
            audit_event(date_str, "EarthquakePipeline", "upload_parquet", start_time, end_time,
                        "Failed", error_message, 0)
            logging.error(error_message)
            raise RuntimeError(error_message)


class UploadToBigquery:
    def __init__(self, project_id, dataset_name, gcs_bucket):
        """
        :param project_id: Google Cloud project ID
        :param dataset_name: BigQuery dataset name where we want to create the table
        :param gcs_bucket: GCS bucket for staging data (temporary or persistent)
        """
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.gcs_bucket = gcs_bucket

    def to_bigquery(self, table_name, df, write_mode="overwrite"):
        """
        Upload a DataFrame to BigQuery.

        :param table_name: Name of the BigQuery table
        :param df: PySpark DataFrame to be uploaded
        :param write_mode: Mode of writing (default: overwrite)
        :return: None
        """
        start_time = datetime.utcnow()  # Start time for auditing
        try:
            # Write DataFrame to BigQuery
            df.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}:{self.dataset_name}.{table_name}") \
                .option("temporaryGcsBucket", self.gcs_bucket) \
                .mode(write_mode) \
                .save()

            record_count = df.count()  # Number of records processed
            end_time = datetime.utcnow()  # End time for auditing

            # Log success
            audit_event(date_str, "EarthquakePipeline", "to_bigquery", start_time, end_time,
                        "Success", None, record_count)
            logging.info(f"Data written to BigQuery table {self.dataset_name}.{table_name}")
        except Exception as e:
            end_time = datetime.utcnow()  # End time for auditing in case of failure
            error_message = f"An error occurred while writing to BigQuery: {e}"
            audit_event(date_str, "EarthquakePipeline", "to_bigquery", start_time, end_time,
                        "Failed", error_message, 0)
            logging.error(error_message)
            raise RuntimeError(error_message)
