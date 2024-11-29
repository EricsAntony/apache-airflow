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
    fileName = 'spa_order.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Fetched {len(extractedData)} spa data from {fileName}.")
        return extractedData
    
    pk_value = "HOTEL#"
    sk_value = "SPA_ORDER#"

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

        values_for_spa_order = []
        values_for_spa_order_treatment = []
        
        spa_ids = fetch_ids('id', 'test_analytics.spa')
        skip_count_no_spa_id = 0
        
        treatment_ids = fetch_ids('id', 'test_analytics.spa_treatment')
        skip_count_no_treatment_id = 0
        
        for dynamo_item in data:
            order_id = dynamo_item.get('id', {}).get('S', '')
            guest_type = dynamo_item.get('guestType', {}).get('S', '')
            no_of_guests = dynamo_item.get('numberOfGuest', {}).get('N', 0)
            no_of_female = dynamo_item.get('femalePax', {}).get('N', 0)
            no_of_male = dynamo_item.get('maleFax', {}).get('N', 0)
            created_at = dynamo_item.get('createdAt', {}).get('N', 0)
            scheduled_date = dynamo_item.get('scheduledDate', {}).get('S', '') 
            scheduled_time = dynamo_item.get('scheduledTime', {}).get('S', '')
            
            datetime_string = f"{scheduled_date} {scheduled_time}"
                        
            amount = dynamo_item.get('totalAmount', {}).get('S', '')
            room_no = dynamo_item.get('roomNo', {}).get('S', '')
            spa_id = dynamo_item.get('spaId', {}).get('S', '')
            booking_id = dynamo_item.get('bookingId', {}).get('S', '')
            items = dynamo_item.get('items', {}).get('L', [])
            
            if spa_id not in spa_ids:
                logging.info(f"Skipping spa id {spa_id} as it does not have a valid spa_id in spa table.")
                skip_count_no_spa_id += 1
                store_skip_data_to_file(dynamo_item, 'spa id not in spa table', 'skipped_spa_order_no_spa_id.json')
                continue
            
            for item in items:
                itemMap = item.get('M', {})
                
                treatment_id = itemMap.get('Id', {}).get('S', '')
                treatment_amount = itemMap.get('amount', {}).get('S', '')
                
                if is_float(treatment_amount):
                    treatment_amount
                else:
                    treatment_amount = 0
                
                if treatment_id not in treatment_ids:
                    logging.info(f"Skipping treatment id {treatment_id} as it does not have a valid treatment id in treatment table.")
                    skip_count_no_treatment_id += 1
                    store_skip_data_to_file(dynamo_item, 'treatment id not in treatment table', 'skipped_spa_order_no_treatment_id.json')
                    continue
                
                values_for_spa_order_treatment.append((
                    order_id,
                    treatment_id,
                    treatment_amount
                ))
                
            try:
                timestamp = int(created_at)
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                logging.error(f"Invalid 'CheckOutTime' timestamp for booking_id {booking_id}.")
                
            try:
                scheduled_timestamp = datetime.strptime(datetime_string, '%Y-%m-%d %H:%M')
            except ValueError as e:
                print(f"Error parsing datetime: {e}")
                scheduled_timestamp = None
                
            if is_float(amount):
                amount
            else:
                amount = 0
                    
            values_for_spa_order.append((
                order_id,
                guest_type,
                no_of_guests,
                no_of_female,
                no_of_male,
                date,
                scheduled_timestamp,
                amount,
                room_no,
                spa_id,
                booking_id                
            ))

        if values_for_spa_order:
            insert_query_spa_order_table = """
                INSERT INTO test_analytics.spa_order (
                    id, guest_type, no_of_guests, no_of_female_guests,
                    no_of_male_guests, created_at, scheduled_date_time, amount, 
                    room_no, spa_id, hotel_booking_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_spa_order_table, values_for_spa_order)
            conn.commit()
            
        if values_for_spa_order_treatment:
            insert_query_spa_order_treatment_table = """
                INSERT INTO test_analytics.spa_order_treatment (
                    spa_order_id, spa_treatment_id, amount
                )
                VALUES %s ON CONFLICT (spa_order_id, spa_treatment_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_spa_order_treatment_table, values_for_spa_order_treatment)
            
        conn.commit()
        logging.info(f"Skipped {skip_count_no_spa_id} records with invalid spa_id.")
        logging.info(f"Skipped {skip_count_no_treatment_id} records with invalid treatment id.")
        logging.info(f"Inserted {len(values_for_spa_order)} values_for_spa_order records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_spa_order_treatment)} values_for_spa_order_treatment records into PostgreSQL.")

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

with DAG('load_spa_order_pipeline', 
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
    
