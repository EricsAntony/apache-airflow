from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from essentials import * 

def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = propertyTable  
    fileName = 'hotel.json'
    
    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData

    pk_value = "GROUP"
    sk_value = "HOTEL#"

    query_params = {
        'TableName': table_name,
        'KeyConditionExpression': '#pk = :pk_val AND begins_with(#sk, :sk_val)',
        'ExpressionAttributeNames': {
            '#pk': 'pk', 
            '#sk': 'sk',
            '#isDeleted': 'isDeleted'  
        },
        'ExpressionAttributeValues': {
            ':pk_val': {'S': pk_value},
            ':sk_val': {'S': sk_value},
            ':isDeleted': {'BOOL': False} 
        },
        'FilterExpression': '#isDeleted = :isDeleted'
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

        brand_ids = fetch_ids('id', 'test_analytics.brand')
        skip_count_no_brand_id = 0
        
        hotel_data = []

        for dynamo_item in data:
            id = dynamo_item.get('id', {}).get('S', '')
            name = dynamo_item.get('name', {}).get('S', '')
            code = dynamo_item.get('code', {}).get('S', '')
            brandId = dynamo_item.get('brandId', {}).get('S', '')
            country = dynamo_item.get('country', {}).get('S', '')
            currency = dynamo_item.get('currency', {}).get('S', '')

            if brandId not in brand_ids:
                logging.info(f"Skipping hotel with brandId {brandId} as it does not exist in the brand table.")
                skip_count_no_brand_id += 1
                store_skip_data_to_file(dynamo_item, 'brand id not exist in brand table', 'skipped_hotel.json')
                continue

            hotel_data.append((id, code, name, country, currency, brandId))

        if hotel_data:
            insert_query = """
                INSERT INTO test_analytics.hotel (
                    id, code, name, country, currency, brand_id
                ) VALUES %s ON CONFLICT (id) DO NOTHING;
            """

            execute_values(cursor, insert_query, hotel_data)
            conn.commit()
            logging.info(f"Skipped {skip_count_no_brand_id} as no corresponding brand id found in brand table.\n\n")
            logging.info(f"Inserted {len(hotel_data)} records into PostgreSQL.")
        else:
            logging.info("No valid records for insertion.")

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

with DAG('load_hotel_pipeline', 
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
