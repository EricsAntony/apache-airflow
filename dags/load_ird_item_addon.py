from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import logging
from datetime import datetime
from essentials import *
from psycopg2 import extras
import os

def pull_data_from_dynamodb(**kwargs):
    fileName = 'ird_item_addon.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} data from {fileName}")
        return extractedData

    client = get_dynamodb_client()
    table_name = propertyTable

    pk_value = "HOTEL#"
    sk_value = "IRD#ADDON#"

    query_params = {
        'TableName': table_name,
        'FilterExpression': 'begins_with(#pk, :pk_val) AND begins_with(#sk, :sk_val)',
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
        response = client.scan(**query_params)
        all_items.extend(response['Items'])
        
        while 'LastEvaluatedKey' in response:
            query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
            response = client.scan(**query_params)
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

        values_for_ird_item_addon = []
        skip_count = 0

        hotel_ids = fetch_ids('id', 'test_analytics.hotel')
        
        for dynamo_item in data:
            item_id = dynamo_item.get('id', {}).get('S', '')
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            name = dynamo_item.get('name', {}).get('S', '')
            code = dynamo_item.get('code', {}).get('S', '')
            
            if hotel_id not in hotel_ids:
                skip_count += 1
                store_skip_data_to_file(dynamo_item, 'hotel not found in hotel table', 'skipped_ird_item_addon.json')
                logging.info(f"Skipping item with ID {item_id} as it belongs to a hotel not in the hotel table. {hotel_id}")
                continue
            
            values_for_ird_item_addon.append((
               item_id,
               name, 
               hotel_id,
               code
            ))

        if values_for_ird_item_addon:
            insert_query_ird_item_table = """
                INSERT INTO test_analytics.ird_item_addon (
                    id, name, hotel_id, code
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_ird_item_table, values_for_ird_item_addon)

        conn.commit()
        logging.info(f"Skipped {skip_count} items as they belong to a hotel not in the hotel table.")
        logging.info(f"Inserted {len(values_for_ird_item_addon)} records into PostgreSQL.")

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

with DAG('load_ird_item_addon_pipeline', 
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
    
