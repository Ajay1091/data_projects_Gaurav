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