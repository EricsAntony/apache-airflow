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
    fileName = 'hk_fail_count.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData
    
    pk_value = "HOTEL#"
    sk_value = "FAILED_HK_ORDER#"

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

        values_for_hk_fail_count = []
        skip_count = 0
        
        hotelIds = fetch_ids("id", "test_analytics.hotel")
            
        for dynamo_item in data:
            hotelId = dynamo_item.get('hotelId', {}).get('S', '')
            date = dynamo_item.get('currentDate', {}).get('S', '')
            count = dynamo_item.get('failureCount', {}).get('N', '')
            
            if hotelId not in hotelIds:
                skip_count += 1
                logging.warning(f"Hotel ID {hotelId} not found in the hotel table.")
                store_skip_data_to_file(dynamo_item, 'hotel no found in the hotel table', 'skipped_hk_fail_count.json')
                continue
            
            values_for_hk_fail_count.append((hotelId, date, count))
            
        if values_for_hk_fail_count:
            insert_query_hk_fail_count_table = """
                INSERT INTO test_analytics.housekeeping_order_failed_count (
                    hotel_id, date, count
                )
                VALUES %s;
            """
            extras.execute_values(cursor, insert_query_hk_fail_count_table, values_for_hk_fail_count)
            
        conn.commit()
        logging.info(f"Skipped {skip_count} records due to missing hotel ID.")
        logging.info(f"Inserted {len(values_for_hk_fail_count)} hk fail count records into PostgreSQL.")

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

with DAG('load_hk_fail_count_pipeline', 
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
    
