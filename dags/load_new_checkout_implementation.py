from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import logging
from datetime import datetime
from dateutil import parser
from essentials import *
from psycopg2 import extras

def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = integrationsTable  
    fileName = 'new_checkout_implenetation.json'
    
    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData

    pk_value = "HOTEL"
    sk_value = "CHECKOUT#"

    query_params = {
        'TableName': table_name,
        'KeyConditionExpression': '#pk = :pk_val AND begins_with(#sk, :sk_val)',
        'ExpressionAttributeNames': {
            '#pk': 'pk',
            '#sk': 'sk',
        },
        'ExpressionAttributeValues': {
            ':pk_val': {'S': pk_value},
            ':sk_val': {'S': sk_value},
        },
    }

    all_items = [] 
    
    try:
        response = client.query(**query_params)
        all_items.extend(response['Items'])
        
        while 'LastEvaluatedKey' in response:
            query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
            response = client.query(**query_params)
            all_items.extend(response['Items'])
        
        logging.info(f"Pulled {len(all_items)} items from DynamoDB")
        
        store_data_to_file(all_items, fileName)
        
        kwargs['ti'].xcom_push(key='dynamo_data', value=all_items)
        
        return all_items

    except Exception as e:
        logging.error(f"Error pulling data from DynamoDB: {str(e)}")
        raise


def insert_data_to_postgresql(ti, **kwargs):
    data = ti.xcom_pull(task_ids='pull_data_from_dynamodb', key='dynamo_data')

    if not data:
        logging.warning("No data received from DynamoDB.")
        return

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        values_for_checkout = []

        for dynamo_item in data:
            booking_id = dynamo_item.get('BookingId', {}).get('S', '')
            created_at = dynamo_item.get('CheckOutTime', {}).get('S', 0)
            channel = dynamo_item.get('Channel', {}).get('S', '')

            try:
                timestamp = int(created_at)
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                logging.error(f"Invalid 'CheckOutTime' timestamp for booking_id {booking_id}")

            values_for_checkout.append((
                booking_id,
                'Success',
                date,
                booking_id, 
                channel
            ))

        if values_for_checkout:
            insert_query_booking_checkout_table = """
                INSERT INTO test_analytics.booking_check_out (
                    id, check_out_status, checked_out_at, hotel_booking_id, channel
                )
                VALUES %s ON CONFLICT(id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_booking_checkout_table, values_for_checkout)

        conn.commit()
        logging.info(f"Inserted {len(values_for_checkout)} records into PostgreSQL.")

    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL: {str(e)}")
        raise

    finally:
        cursor.close()
        conn.close()
    
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def convert_to_date(date_str):
    try:
        date_obj = parser.parse(date_str)
        
        return date_obj.strftime("%Y-%m-%d")
    
    except (ValueError, TypeError) as e:
        logging.error(f"Cannot parse date string '{date_str}': {e}")
        return None

with DAG('load_checkout_new_implementation_pipeline', 
         default_args=default_args, 
         schedule_interval='@daily',  
         catchup=False) as dag:

    
    pull_data = PythonOperator(
        task_id='pull_data_from_dynamodb',
        python_callable=pull_data_from_dynamodb,
        provide_context=True,  
    )

    
    insert_data = PythonOperator(
        task_id='insert_data_to_postgresql',
        python_callable=insert_data_to_postgresql,
        provide_context=True,  
    )

    
    pull_data >> insert_data
    
