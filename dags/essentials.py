import boto3
import logging
import json
import os
from datetime import datetime
from psycopg2 import *
import psycopg2
from dateutil import parser

env = 'uat'

propertyTable = 'properties-' + env + '-table'
integrationsTable = 'integrations-' + env + '-table'

def get_postgres_connection():
    db_host = 'analytics.cxwyoy2imfyo.us-east-1.rds.amazonaws.com'
    db_name = 'sampledatabase'
    db_user = 'postgres'
    db_password = 'B3RwuvCp6AAnmpe1bHDt'
    db_port = 5432  

    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {str(e)}")
        return None
    return conn
        
def get_dynamodb_client():
    return boto3.client('dynamodb', region_name='ap-south-1', aws_access_key_id='ASIAUBCWNNSUXJII6SFD',
                        aws_secret_access_key='EtFPb59MWD3PpJdmwLI0EsF/0q/8ZXNuC3YLcbDr',
                        aws_session_token='IQoJb3JpZ2luX2VjEMr//////////wEaDmFwLXNvdXRoZWFzdC0xIkYwRAIgdMUg82rs6BMayxA6FWvtowGtKa8VWesv212oEayaQaICIFtkJ1ZuDHVyzbbx5ealUrhtxonkAgT7Y2gmXs11EO2pKooDCHMQARoMMjc3MjA2NjI1NDQ5Igz3TX/fP+tadr6N08kq5wJ/A4xT+HNXIz32eEkRdGMRXVEK4co6z+sjv0SuPG5HfBpJKJElKxRfYV3W4OWtvCOuCKA80K9Zd6eLuUvmET5LsGiAOZImVHeO6QrM6nOtrS5Evde16SIzh+RwVFIh4IYhrU+o7T+WzqHzVD7qYlYSV45b3CQEI1z8sJF5U/Gs7V5UfAvl7MoFxbWmoM9SVvDSm0B9TpszvQIZ+0csCBl67+tlg3G03EK3n/VoerL/atGTaEp3jCvLA0ho3fCHn46OU2XxAUFmW6jrHmqS0mXBLM3ssL5vzXG20KasMNJQNcEZ7E+sgtWQWyTwb5Z67no17liJoh7+NO5FOzYI/YJfdDFuxHL4DPOOP4awFCFzOKn+frDJnRUKmzNdHAylQrnkDHgfFpq5xunptEouKeZCb1YU29JJfZBZRwktFf3+8io6w8WpeD6vi+oOUBTl+aRzRA/rkjj5vEWJ7RJap1Jkuy+2Q9T4BzCop6a6BjqnAXyZTx0DRQsYxlsQUr+6BC+sa8fZBYiHEoKwfkji/udBrEBJljhOC8wYdYunWlE8ee8KPKNrlEm/WDpna2pkBaQFI7SXH1r9OSrPR2GnczkexuGMYPgp/J2SyKbI/X76RdiO4GvpYisKaVw4lqG1U5KuNexcbTTihqgfxnwngeiJ7h8EBJpDtvA4wHiByx2hRgYn4IWVwcRG9Avw1jtF9y0bzxxJGs8L')  

def store_data_to_file(data, filename='dynamo_data.json'):
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"Data has been stored to {filename}.")
    except Exception as e:
        logging.error(f"Error storing data to file: {str(e)}")
        
def store_skip_data_to_file(data, reason, filename='skip.json'):
    try:
        if not os.path.exists('skipped_data'):
            os.makedirs('skipped_data')
            logging.info("Created 'skipped_data' directory.")

        file_path = os.path.join('skipped_data', filename)

        with open(file_path, 'a') as f:
            json.dump(reason + ": ", f, indent=2)
            json.dump(data, f, indent=4)
        
    except Exception as e:
        logging.error(f"Error storing data to file: {str(e)}")
        
def extract_data_from_file(filename, **kwargs):

    data, flag = None, False

    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try:
                all_items = json.load(f)
                if all_items:
                    logging.info(f"Found existing data file {filename}, loading data from file.")
                    kwargs['ti'].xcom_push(key='dynamo_data', value=all_items)
                    data, flag = all_items, True 
                else:
                    logging.warning(f"File {filename} is empty, pulling data from DynamoDB.")
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode {filename}, pulling data from DynamoDB.")
    
    else:
        logging.warning(f"File {filename} does not exist, pulling data from DynamoDB.")
    
    return data, flag

def parse_timestamp(timestamp_str):
    try:
        if timestamp_str:
            
            return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S") 
        else:
            return None
    except (ValueError, TypeError) as e:
        logging.error(f"Invalid timestamp format: {timestamp_str}. Error: {e}")
        return None 
    
def is_float(s):
    try:
        float(s)  
        return True
    except ValueError:
        return False
    
def fetch_ids(columnList, tableName):
    
    conn = get_postgres_connection()
    
    cursor = conn.cursor()
    
    query = "SELECT "+columnList+" FROM "+tableName
    cursor.execute(query)
    result = cursor.fetchall()
    ids = [row[0] for row in result]

    logging.info(f"Fetched {len(ids)} records from {tableName}.")
    logging.info(f"ids: {ids}")
    return ids

def convert_to_date(date_str):
    try:
        if date_str == '' or str.isspace(date_str):
            logging.error(f"Cannot parse date string '{date_str}'")
            return None
        
        date_obj = parser.parse(date_str)
        
        return date_obj.strftime("%Y-%m-%d")
    
    except (ValueError, TypeError) as e:
        logging.error(f"Cannot parse date string '{date_str}': {e}")
        return None