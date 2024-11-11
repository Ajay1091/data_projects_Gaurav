import requests
import os
import logging
import datetime
import json
import re



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


# def transformation(element):
#     for values in element:
#         try:
#             # Validate processed_entry (ensure it's not None)
#             if values is None:
#                 raise ValueError("Processed entry is None")
#
#             # Handle missing 'time' and 'updated' values and convert them to UTC
#             time = float(values.get('time', None)) / 1000 if values.get('time') else None
#             if time:
#                 values['time'] = datetime.datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
#             else:
#                 values['time'] = None
#
#             update = float(values.get('updated', None)) / 1000 if values.get('updated') else None
#             if update:
#                 values['updated'] = datetime.datetime.utcfromtimestamp(update).strftime('%Y-%m-%d %H:%M:%S')
#             else:
#                 values['updated'] = None
#
#             # Handle 'area' extraction from 'place'
#             place = values.get('place', '')
#             match = re.search(r"of\s+(.+)$", place)
#             values['area'] = match.group(1).strip() if match else None
#
#             # Set 'insert_date' to the current UTC timestamp
#             values['insert_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
#
#             # Check if values dictionary has valid values
#             earthquake_dic = {
#                 "mag": values.get("mag", None),  # Use None for missing value
#                 "place": values.get("place", None),  # Use None for missing value
#                 "time": values.get("time", None),  # Use None for missing value
#                 "updated": values.get("updated", None),  # Use None for missing value
#                 "tz": values.get("tz", None),  # Use None for missing value
#                 "url": values.get("url", None),  # Use None for missing value
#                 "detail": values.get("detail", None),  # Use None for missing value
#                 "felt": values.get("felt", None),  # Use None for missing value
#                 "cdi": values.get("cdi", None),  # Use None for missing value
#                 "mmi": values.get("mmi", None),  # Use None for missing value
#                 "alert": values.get("alert", None),  # Use None for missing value
#                 "status": values.get("status", None),  # Use None for missing value
#                 "tsunami": values.get("tsunami", None),  # Use None for missing value
#                 "sig": values.get("sig", None),  # Use None for missing value
#                 "net": values.get("net", None),  # Use None for missing value
#                 "code": values.get("code", None),  # Use None for missing value
#                 "ids": values.get("ids", None),  # Use None for missing value
#                 "sources": values.get("sources", None),  # Use None for missing value
#                 "types": values.get("types", None),  # Use None for missing value
#                 "nst": values.get("nst", None),  # Use None for missing value
#                 "dmin": values.get("dmin", None),  # Use None for missing value
#                 "rms": values.get("rms", None),  # Use None for missing value
#                 "gap": values.get("gap", None),  # Use None for missing value
#                 "magType": values.get("magType", None),  # Use None for missing value
#                 "type": values.get("type", 'earthquake'),  # Use default value 'earthquake' if None
#                 "title": values.get("title", None),  # Use None for missing value
#                 "area": values.get("area", None),  # Use None for missing value
#                 "longitude": values.get("longitude", None),  # Use None for missing value
#                 "latitude": values.get("latitude", None),  # Use None for missing value
#                 "depth": values.get("depth", None),  # Use None for missing value
#                 "insert_date": values.get("insert_date", datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))  # Use current timestamp if None
#             }
#
#             # Yield the processed data
#             yield earthquake_dic
#
#         except Exception as e:
#             logging.error(f"Error processing element: {e}")

def transformation(element):
    for values in element:
        try:
            # Convert 'time' and 'updated' to UTC format if possible
            time = float(values.get('time', 0)) / 1000
            values['time'] = datetime.datetime.utcfromtimestamp(time).strftime(
                '%Y-%m-%d %H:%M:%S') if time > 0 else None

            update = float(values.get('updated', 0)) / 1000
            values['updated'] = datetime.datetime.utcfromtimestamp(update).strftime(
                '%Y-%m-%d %H:%M:%S') if update > 0 else None

            # Extract 'area' from 'place'
            place = values.get('place', '')
            match = re.search(r"of\s+(.+)$", place)
            values['area'] = match.group(1).strip() if match else None

            # Set 'insert_date' to current UTC time
            values['insert_date'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

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


