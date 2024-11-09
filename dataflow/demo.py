import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os
import requests
import logging
import datetime
import json
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    try:
        # Set environment variable for Google credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\HP\OneDrive\Desktop\GCP_prac\earthquake-project\keys\all-purpuse-41a4e2945880.json'
        options = PipelineOptions()

        # Configure Google Cloud options
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = 'all-purpuse'
        google_cloud_options.job_name = 'historical_data'
        google_cloud_options.region = 'us-central1'
        google_cloud_options.staging_location = 'gs://earthquake-project-dataflow/stagging'
        google_cloud_options.temp_location = 'gs://earthquake-project-dataflow/temp'

        # Retrieve data from the URL
        url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
        response = requests.get(url)
        if response.status_code == 200:
            content = response.json()
            logging.info(f"Data successfully retrieved from {url}")
        else:
            logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
            raise Exception(f"HTTP request failed with status {response.status_code}")

        content_as_string = json.dumps(content)

        # Define file paths
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        file_path = f'gs://earthquake-project-main-bucket/raw_dataflow/{date_str}.json'

        # Apache Beam Pipeline to write raw data to GCS
        with beam.Pipeline(options=options) as p:
            _ = (
                p
                | 'Create PCollection' >> beam.Create([content_as_string])
                | 'Write to GCS' >> beam.io.WriteToText(file_path, file_name_suffix='.json', shard_name_template='', num_shards=1)
            )

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

        # Transformation function for data processing
        def transformation(element):
            for values in element:
                try:
                    time = float(values.get('time', 0)) / 1000
                    values['time'] = datetime.datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S') if time > 0 else None
                    update = float(values.get('updated', 0)) / 1000
                    values['updated'] = datetime.datetime.utcfromtimestamp(update).strftime('%Y-%m-%d %H:%M:%S') if update > 0 else None

                    place = values.get('place', '')
                    match = re.search(r"of\s+(.+)$", place)
                    values['area'] = match.group(1).strip() if match else None

                    values['insert_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                    yield values
                except Exception as e:
                    logging.error(f"Error processing element: {e}")

        # Silver layer and BigQuery write paths
        silver_file_path = f'gs://earthquake-project-main-bucket/silver_dataflow/{date_str}.json'
        bq_table_name = 'all-purpuse.earthquake_project.earthquake_dataflow'

        # Pipeline for silver layer transformation and writing to GCS
        with beam.Pipeline(options=options) as p:
            silver_layer = (
                p
                | 'Read Raw Data' >> beam.io.ReadFromText(file_path)
                | 'Flatten Features' >> beam.Map(feature_flatten)
                | 'Transform Data' >> beam.FlatMap(transformation)
                | 'Write Silver Data to GCS' >> beam.io.WriteToText(silver_file_path, file_name_suffix='.json', shard_name_template='', num_shards=1)
            )

            # Pipeline for BigQuery write
            bq_pipeline = (
                p
                | 'Read Silver Data' >> beam.io.ReadFromText(silver_file_path)
                | 'Flatten for BQ' >> beam.Map(feature_flatten)
                | 'Transform for BQ' >> beam.FlatMap(transformation)
                | 'Write to BigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
                    table=bq_table_name,
                    schema='SCHEMA_AUTODETECT',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
