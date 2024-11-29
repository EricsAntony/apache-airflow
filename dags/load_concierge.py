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
    fileName = 'concierge.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        return extractedData
    
    pk_value = "HOTEL#"
    sk_value = "CONCIERGE#"

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

        values_for_concierge_service_category = []
        values_for_concierge_service = []
        values_for_concierge_service_item = []
        
        hotel_ids = fetch_ids('id', 'test_analytics.hotel')
        skip_count_no_hotel_id = 0
        
        item_id = 0

        for dynamo_item in data:
            code = dynamo_item.get('code', {}).get('S', '')
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            concierge_id = dynamo_item.get('id', {}).get('S', '')
            items = dynamo_item.get('items', {}).get('L', [])
            name = dynamo_item.get('name', {}).get('S', '')
            
            if hotel_id not in hotel_ids:
                logging.info(f"concierge service skipped because {hotel_id} is not in hotel table")
                store_skip_data_to_file(dynamo_item, 'no hotel found', 'skipped_concierge.json')
                skip_count_no_hotel_id += 1
                continue
            
            for item in items:
                itemMap = item.get('M', {})
                item_name = itemMap.get('name', {}).get('S', '')
                item_id = itemMap.get('id', {}).get('S', '')
                
                values_for_concierge_service_item.append((
                   item_id,
                   item_name,
                   concierge_id
                ))
                
            values_for_concierge_service.append((
                concierge_id,
                code,
                name,
                'CONCIERGE_'+hotel_id
            ))
            
            values_for_concierge_service_category.append((
                'CONCIERGE_'+hotel_id,
                'CONCIERGE',
                'concierge',
                hotel_id
            ))

        if values_for_concierge_service_category:
            insert_query_hk_service_category_table = """
            INSERT INTO test_analytics.housekeeping_service_category (
                    id, code, name, hotel_id
                )
                VALUES %s ON CONFLICT(id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_hk_service_category_table, values_for_concierge_service_category)
            
        if values_for_concierge_service:
            insert_query_hk_service_table = """
                INSERT INTO test_analytics.housekeeping_service (
                    id, code, name, housekeeping_service_category_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_hk_service_table, values_for_concierge_service)
            
        if values_for_concierge_service_item:
            insert_query_hk_service_item_table = """
                INSERT INTO test_analytics.housekeeping_service_item (
                    id, name, housekeeping_service_id
                )
                VALUES %s ON CONFLICT(id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_hk_service_item_table, values_for_concierge_service_item)
            
        conn.commit()
        logging.info(f"Skipped {skip_count_no_hotel_id} records due to missing hotel ID.")
        logging.info(f"Inserted {len(values_for_concierge_service)} values_for_concierge_service records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_concierge_service_category)} values_for_concierge_service_category records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_concierge_service_item)} values_for_concierge_service_item records into PostgreSQL.")

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

with DAG('load_concierge_pipeline', 
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
    
