##########################################################################################
"""
project_name = earthquake-project-dataproc
file_name = utility.py
date - 2024-10-21
desc - reusable class and function
version - 1
date of modification -
"""

###########################################################################################

## importing the libraries and module required


from pyspark.sql import SparkSession
import requests
import os
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, BooleanType,FloatType
from datetime import datetime
import json
import pyspark.sql.functions as f
import logging
from google.cloud import bigquery
import configuration as conf



#### defining reusable class and functions


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



### Google cloud access key
# Set the environment variable for Google Cloud credentials
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = conf.GOOGLE_APPLICATION_CREDENTIALS


### class for reading data from api
class ReadDataFromApiJson:
    @staticmethod
    def reading(url):
        """

        :param url: API calling url daily / historical
        :return: data in the form of dictionary
        """
        try:
            response = requests.get(url)

            if response.status_code == 200:
                content = response.json()  # Get the content of the URL
                logging.info(f"Data successfully retrieved from {url}")
                return content
            else:
                logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred while retrieving data from {url}: {e}")


class UploadtoGCS:
    @staticmethod
    def uploadjson(bucket_name, data_object):
        """

        :param bucket_name: bucket name where we want to store data
        :param data_object: the json object
        :return: None
        """
        """Uploads a JSON object to a GCS bucket."""
        try:
            # Initialize GCS client and bucket
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Create a unique file name with the current date
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"{date_str}.json"

            # Specify the blob (path) within the bucket where the file will be stored
            destination_blob_name = f"landing_layer/{filename}"
            blob = bucket.blob(destination_blob_name)

            # Upload the JSON data directly from memory to GCS
            blob.upload_from_string(data=json.dumps(data_object), content_type='application/json')

            logging.info(f"Upload of {filename} to {destination_blob_name} complete.")
        except Exception as e:
            logging.error(f"Error uploading JSON to GCS: {e}")
    @staticmethod
    def uploadjson_daily(bucket_name, data_object):
        """

        :param bucket_name: bucket name where we want to store data
        :param data_object: the json object
        :return: None
        """
        """Uploads a JSON object to a GCS bucket."""
        try:
            # Initialize GCS client and bucket
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Create a unique file name with the current date
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f"{date_str}.json"

            # Specify the blob (path) within the bucket where the file will be stored
            destination_blob_name = f"landing_layer/daily{filename}"
            blob = bucket.blob(destination_blob_name)

            # Upload the JSON data directly from memory to GCS
            blob.upload_from_string(data=json.dumps(data_object), content_type='application/json')

            logging.info(f"Upload of {filename} to {destination_blob_name} complete.")
        except Exception as e:
            logging.error(f"Error uploading JSON to GCS: {e}")



class GCSFileDownload:
    def __init__(self, bucket_name):
        """
        :param bucket_name: bucket name where your json file is store
        """
        """Initialize the class with bucket information."""
        self.storage_client = storage.Client()  # Uses the environment variable for credentials
        self.bucket_name = bucket_name

    def download_json_as_text(self, json_file_path):
        """
        :param json_file_path: file path where json file is store
        :return: json in dictionary form
        """
        """Download a JSON file from GCS and return it as a Python dictionary."""
        try:
            # Access the GCS bucket
            bucket = self.storage_client.bucket(self.bucket_name)

            # Retrieve the JSON file from the bucket
            blob = bucket.blob(json_file_path)

            # Download the JSON data as a text string
            json_data = blob.download_as_text()

            # Convert the JSON string to a Python dictionary
            json_dict = json.loads(json_data)

            logging.info(f"Successfully downloaded {json_file_path} from GCS.")

            # Return the JSON data as a dictionary
            return json_dict
        except Exception as e:
            logging.error(f"Error downloading {json_file_path} from GCS: {e}")





class EarthquakeDataFrameCreation:
    def __init__(self, json_data):
        """
        :param json_data: data from GCS bucket in Json form
        """
        self.json_data = json_data
        self.spark = SparkSession.builder.appName('Earthquake Data Processor').getOrCreate()

        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def process_data(self):
        """
        :return: raw data
        """
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

                # Update properties with geometry
                #properties.update(geometry)
                data.append(properties)

            return data
        except KeyError as e:
            self.logger.error('KeyError: %s', e)

        except Exception as e:
            self.logger.error('An error occurred while processing data: %s', e)

    def convert_to_dataframe(self):
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
                    StructField("geometry", StructType([
                        StructField("longitude", FloatType(), True),
                        StructField("latitude", FloatType(), True),
                        StructField("depth", FloatType(), True)
                    ]), True)
                ])

            processed_data = []
            for entry in data:
                processed_entry = {}
                for key, value in entry.items():
                        # Convert fields to float where necessary
                    if key in ['mag', 'cdi', 'mmi', 'dmin', 'rms', 'gap']:
                        processed_entry[key] = float(value) if value is not None else None
                    elif key == 'geometry':
                            # Add geometry data if available
                        processed_entry['geometry'] = {
                                'longitude': float(value['longitude']) if value['longitude'] is not None else None,
                                'latitude': float(value['latitude']) if value['latitude'] is not None else None,
                                'depth': float(value['depth']) if value['depth'] is not None else None
                            }
                    else:
                        processed_entry[key] = value
                    processed_data.append(processed_entry)

                # Create Spark DataFrame
            df = self.spark.createDataFrame(processed_data, schema)
            self.logger.info('DataFrame created successfully with %d records', df.count())
            return df
        except Exception as e:
            self.logger.error('An error occurred while converting data to DataFrame: %s', e)

        # """
        # :return: final data frame
        # """
        # try:
        #     data = self.process_data()

            # schema = StructType([
            #     StructField("mag", FloatType(), True),
            #     StructField("place", StringType(), True),
            #     StructField("time", StringType(), True),
            #     StructField("updated", StringType(), True),
            #     StructField("tz", StringType(), True),
            #     StructField("url", StringType(), True),
            #     StructField("detail", StringType(), True),
            #     StructField("felt", IntegerType(), True),
            #     StructField("cdi", FloatType(), True),
            #     StructField("mmi", FloatType(), True),
            #     StructField("alert", StringType(), True),
            #     StructField("status", StringType(), True),
            #     StructField("tsunami", IntegerType(), True),
            #     StructField("sig", IntegerType(), True),
            #     StructField("net", StringType(), True),
            #     StructField("code", StringType(), True),
            #     StructField("ids", StringType(), True),
            #     StructField("sources", StringType(), True),
            #     StructField("types", StringType(), True),
            #     StructField("nst", IntegerType(), True),
            #     StructField("dmin", FloatType(), True),
            #     StructField("rms", FloatType(), True),
            #     StructField("gap", FloatType(), True),
            #     StructField("magType", StringType(), True),
            #     StructField("type", StringType(), True),
            #     StructField("title", StringType(), True),
            #     StructField("longitude", FloatType(), True),
            #     StructField("latitude", FloatType(), True),
            #     StructField("depth", FloatType(), True)
            # ])

        #
        #     schema = StructType([
        #         StructField("mag", FloatType(), True),
        #         StructField("place", StringType(), True),
        #         StructField("time", StringType(), True),
        #         StructField("updated", StringType(), True),
        #         StructField("tz", StringType(), True),
        #         StructField("url", StringType(), True),
        #         StructField("detail", StringType(), True),
        #         StructField("felt", IntegerType(), True),
        #         StructField("cdi", FloatType(), True),
        #         StructField("mmi", FloatType(), True),
        #         StructField("alert", StringType(), True),
        #         StructField("status", StringType(), True),
        #         StructField("tsunami", IntegerType(), True),
        #         StructField("sig", IntegerType(), True),
        #         StructField("net", StringType(), True),
        #         StructField("code", StringType(), True),
        #         StructField("ids", StringType(), True),
        #         StructField("sources", StringType(), True),
        #         StructField("types", StringType(), True),
        #         StructField("nst", IntegerType(), True),
        #         StructField("dmin", FloatType(), True),
        #         StructField("rms", FloatType(), True),
        #         StructField("gap", FloatType(), True),
        #         StructField("magType", StringType(), True),
        #         StructField("type", StringType(), True),
        #         StructField("title", StringType(), True),
        #         StructField("geometry", StructType([
        #             StructField("longitude", FloatType(), True),
        #             StructField("latitude", FloatType(), True),
        #             StructField("depth", FloatType(), True)
        #         ]), True)
        #     ])
        #
        #     processed_data = []
        #     for entry in data:
        #         processed_entry = {}
        #         for key, value in entry.items():
        #             # Convert fields to float where necessary
        #             if key in ['mag', 'cdi', 'mmi', 'dmin', 'rms', 'gap']:
        #                 processed_entry[key] = float(value) if value is not None else None
        #             else:
        #                 processed_entry[key] = value
        #         processed_data.append(processed_entry)
        #
        #
        #
        #     # Create Spark DataFrame
        #     df = self.spark.createDataFrame(processed_data, schema)
        #     self.logger.info('DataFrame created successfully with %d records', df.count())
        #     return df
        # except Exception as e:
        #     self.logger.error('An error occurred while converting data to DataFrame: %s', e)

class Transformation:

    def process(df):
        """
        :return: transformed df
        """
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

            return df_transformed
        except Exception as e:
            raise RuntimeError(f'An error occurred during DataFrame transformation: {e}')





class SilverParquet:
    @staticmethod
    def upload_parquet(df, GCS_path):
        """
        Uploads the DataFrame to a Parquet file in Google Cloud Storage.

        :param df: DataFrame to upload to GCS as a Parquet file
        :param GCS_path: The GCS path (gs://your-bucket/path/to/output/)
        :return: None
        """

        try:
            # Write the DataFrame to Parquet
            df.write.parquet(GCS_path)
            logging.info(f"Successfully uploaded DataFrame to {GCS_path}")

        except Exception as e:
            logging.error(f"Error uploading DataFrame: {e}")




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
        try:
            # Configure Spark session to use GCS bucket for BigQuery staging
            df.write \
                .format("bigquery") \
                .option("table", f"{self.project_id}:{self.dataset_name}.{table_name}") \
                .option("temporaryGcsBucket", self.gcs_bucket).mode(write_mode) \
                .save()

            logging.info(f"Data written to BigQuery table {self.dataset_name}.{table_name}")

        except Exception as e:
            logging.error(f"An error occurred while writing to BigQuery: {e}")
            raise RuntimeError(f"An error occurred while writing to BigQuery: {e}")