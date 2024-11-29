from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import logging
from datetime import datetime
from essentials import *
from psycopg2 import extras

def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = propertyTable
    fileName = 'spa.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData
    
    pk_value = "HOTEL#"
    sk_value = "SPA#"

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

        values_for_spa = []
        
        hotel_ids = fetch_ids('id', 'test_analytics.hotel')
        skip_count_no_hotel_id = 0
        
        for dynamo_item in data:
            code = dynamo_item.get('code', {}).get('S', '')
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            spa_id = dynamo_item.get('id', {}).get('S', '')
            name = dynamo_item.get('name', {}).get('S', '')
            
            if hotel_id not in hotel_ids:
                logging.warning(f"Skipped item with hotel_id {hotel_id} because it doesn't exist in the 'hotel' table.")
                skip_count_no_hotel_id += 1
                store_skip_data_to_file(dynamo_item, 'no valid hotel in hotel table', 'skipped_spa.json')
                continue
            
            values_for_spa.append((
                spa_id,
                code,
                name,
                hotel_id
            ))

        if values_for_spa:
            insert_query_spa_table = """
                INSERT INTO test_analytics.spa (
                    id, code, name, hotel_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_spa_table, values_for_spa)
            
        conn.commit()
        logging.info(f"Skipped {skip_count_no_hotel_id} because there are no hotels in the database\n\n")
        logging.info(f"Inserted {len(values_for_spa)} values_for_spa records into PostgreSQL.")

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

with DAG('load_spa_pipeline', 
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
    
