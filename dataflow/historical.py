###########################################################################################################
"""
file_name = historical.py
desc = manual_data ingection by using datflow in bigquary
date = 2024/11/05
version = 1

"""
############################################################################################################


## import library

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import configuration as conf
from utility import ReadDataFromApiJson,feature_flatten,transformation
from apache_beam.io.gcp.gcsio import GcsIO
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import os
import pandas as pd

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    try:
        # Set environment variable for Google credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = conf.GOOGLE_APPLICATION_CREDENTIALS
        options = PipelineOptions()

        # Configure Google Cloud options
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = conf.PROJECT_NAME
        google_cloud_options.job_name = conf.JOB_NAME_HISTORICAL
        google_cloud_options.region = conf.LOCATION
        google_cloud_options.staging_location = conf.STAGGING
        google_cloud_options.temp_location = conf.TEMP



        ## data read from api

        # content = ReadDataFromApiJson.reading(conf.URL_MONTH)
        #
        # content_as_string = json.dumps(content)

        ### load json data to GCS bucket
        # with beam.Pipeline(options=options) as p:
        #     file_GCS  = (
        #         p
        #         | 'Create PCollection' >> beam.Create([content_as_string])
        #         | 'Write to GCS' >> beam.io.WriteToText(conf.RAW_LAYER_PATH, file_name_suffix='.json', shard_name_template='', num_shards=1)
        #          )

        ### load from GCS bucket and flatten and do trnsformation and transfer to GCS

        schema = pa.schema([
            ('mag', pa.float64()),
            ('place', pa.string()),
            ('time', pa.string()),
            ('updated', pa.string()),
            ('tz', pa.null()),
            ('url', pa.string()),
            ('detail', pa.string()),
            ('felt', pa.float64()),
            ('cdi', pa.float64()),
            ('mmi', pa.float64()),
            ('alert', pa.null()),
            ('status', pa.string()),
            ('tsunami', pa.int64()),
            ('sig', pa.int64()),
            ('net', pa.string()),
            ('code', pa.string()),
            ('ids', pa.string()),
            ('sources', pa.string()),
            ('types', pa.string()),
            ('nst', pa.int64()),
            ('dmin', pa.float64()),
            ('rms', pa.float64()),
            ('gap', pa.float64()),
            ('magType', pa.string()),
            ('type', pa.string()),
            ('title', pa.string()),
            ('longitude', pa.float64()),
            ('latitude', pa.float64()),
            ('depth', pa.float64()),
            ('area', pa.string()),
            ('insert_date', pa.string())
        ])


        # Function to handle nulls in the element before converting to Parquet
        def to_parquet(element):
            # Convert each field to its expected type and replace None values with default values or pd.NA for nullable fields
            for field in schema:
                key = field.name
                if key not in element or element[key] is None:
                    # Replace None based on the type in schema
                    if pa.types.is_float64(field.type):
                        element[key] = pd.NA  # for nullable floats
                    elif pa.types.is_int64(field.type):
                        element[key] = pd.NA  # for nullable integers
                    elif pa.types.is_string(field.type):
                        element[key] = ""  # Default to empty string for strings
                    else:
                        element[key] = pd.NA  # General null placeholder for unknown types

            # Create a buffer to hold the Parquet file
            buffer = io.BytesIO()
            # Create a PyArrow table from JSON data
            table = pa.Table.from_pandas(pd.DataFrame([element]), schema=schema, preserve_index=False)
            # Write the table to Parquet format
            pq.write_table(table, buffer)
            # Return the Parquet data as a binary object
            return buffer.getvalue()

        with beam.Pipeline(options=options) as p:
            trnasform_data = (
                p
                | 'Read Raw Data' >> beam.io.ReadFromText(conf.RAW_LAYER_PATH_READ)
                | 'Flatten Features' >> beam.Map(feature_flatten)
                | 'Transform Data' >> beam.FlatMap(transformation)
              #  | 'Write Silver Data to GCS' >> beam.io.WriteToText(silver_file_path, file_name_suffix='.json', shard_name_template='', num_shards=1)
              #   | 'print' >> beam.Map(print)
                | "Convert to Parquet" >> beam.Map(to_parquet)
                | "Write to GCS" >> beam.io.WriteToText(conf.WRITE_PARQUATE, file_name_suffix='.parquet', shard_name_template='', num_shards=1)

            )



    except Exception as e:
        print(f"Pipeline failed: {e}")
