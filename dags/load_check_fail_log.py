from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from dateutil import parser
from essentials import * 


def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = integrationsTable  
    fileName = 'load_check_fail_log.json'
    
    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        return extractedData
    pk_value = "HOTEL#"
    sk_value = "CHECKIN_FAIL_LOG#"

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
        cursor = conn.cursor()

        # Prepare a list of tuples for bulk insert
        bulk_data = []
        for dynamo_item in data:
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            booking_id = dynamo_item.get('bookingId', {}).get('S', '')
            created_at = dynamo_item.get('createdAt', {}).get('N', 0)
            checkin_type = dynamo_item.get('type', {}).get('S', '')
            message = dynamo_item.get('message', {}).get('S', '')
            failId = dynamo_item.get('id', {}).get('S', '')

            if created_at.isdigit():  
                timestamp = int(created_at)  
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')  
            else:
                date = None  

            logging.info(f"Formatted date: {date}")

            # Collect data for bulk insertion
            bulk_data.append((
                failId,
                hotel_id,
                booking_id,
                message,
                checkin_type,
                date
            ))

        if bulk_data:
            # Use execute_values for bulk insert
            insert_query = """
                INSERT INTO test_analytics.booking_check_in_fail_log (
                    fail_id, hotel_id, booking_id, message, type, created_at
                ) VALUES %s;
            """

            # Perform the bulk insert
            execute_values(cursor, insert_query, bulk_data)
            conn.commit()
            logging.info(f"Inserted {len(bulk_data)} records into PostgreSQL.")

        else:
            logging.info("No matching records for insertion.")

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
        # Use dateutil.parser to automatically detect and parse the date string
        date_obj = parser.parse(date_str)
        
        # Return the date in 'YYYY-MM-DD' format
        return date_obj.strftime("%Y-%m-%d")
    
    except (ValueError, TypeError) as e:
        logging.error(f"Cannot parse date string '{date_str}': {e}")
        # If conversion fails, return None
        return None


with DAG('load_hotel_checkin_precheckin_fail_log_pipeline', 
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
