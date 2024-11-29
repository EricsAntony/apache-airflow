from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime
from essentials import *
from psycopg2 import extras

def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = propertyTable
    fileName = 'hk_order.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData
    
    pk_value = "HOTEL#"
    sk_value = "HOUSEKEEPING_ORDER#"

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

        logging.info(f"Connection established to postgres db. Running operation...")

        values_for_hk_order = []
        values_for_hk_order_service_item = []
        
        hotel_ids = fetch_ids("id", "test_analytics.hotel")
        skipped_count_no_hotel = 0

        housekeeping_service_item_ids = fetch_ids("id", "test_analytics.housekeeping_service_item")
        skip_count_no_service_item_id = 0

        query = """SELECT hsi.id, hotel_id, hsi.name FROM test_analytics.housekeeping_service_category hsc, test_analytics.housekeeping_service hs, test_analytics.housekeeping_service_item hsi WHERE hs.housekeeping_service_category_id = hsc.id AND hsi.housekeeping_service_id = hs.id"""
        result = []
        try:
            cursor.execute(query)
            result = cursor.fetchall()
                
        except Exception as e:
            logging.info(f"error: {e}")
            raise
        
        logging.info(f"result: {result}")
        
        for dynamo_item in data:
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            hk_order_id = dynamo_item.get('id', {}).get('S', '')
            booking_id = dynamo_item.get('bookingId', {}).get('S', '')
            created_at = dynamo_item.get('createdAt', {}).get('N', 0)
            room_no = dynamo_item.get('roomNo', {}).get('S', '')
            resolved_time = dynamo_item.get('completedTime', {}).get('S', '')
            items = dynamo_item.get('items', {}).get('L', [])

            if hotel_id not in hotel_ids:
                logging.info(f"No hotel found for {hotel_id} in hotel table")
                store_skip_data_to_file(dynamo_item, 'no hotel in hotel table', 'skipped_hk_order_no_hotel_id.json')
                skipped_count_no_hotel += 1
                continue
            
            for item in items:
                itemMap = item.get('M', {})
                item_amount = itemMap.get('price', {}).get('N', 0)
                item_quantity = itemMap.get('quantity', {}).get('N', 0)
                item_name = itemMap.get('name', {}).get('S', '')
                item_id = itemMap.get('Id', {}).get('S', '')
                
                # for some record item id is there in dynamo, but for others it is not.
                # So, if id is available, we check if id exist in housekeeping_service_item table.
                # else check whether record exists in housekeeping_service_item table using hotel id and name.
                if item_id == '' or item_id is None or str.isspace(item_id):
                    logging.info(f"no item id from record. Going with fetching id from service item table... itemName = {item_name}, hotelId = {hotel_id}")
                    for row in result:
                        if row[1] == hotel_id and row[2] == item_name:
                            item_id = row[0]
                            break
                        else:
                            item_id = None
                else:
                    logging.info(f"item id is present. {item_id}")
                    if item_id not in housekeeping_service_item_ids:
                        logging.info(f"No item found for {item_id} in housekeeping_service_item table")
                        store_skip_data_to_file(dynamo_item, 'no item in hk service item table', 'skipped_hk_order_no_item_id.json')
                        skip_count_no_service_item_id += 1
                        continue
                
                logging.info(f"Inserting data into hk_order_service_item table for hk_order_id = {hk_order_id}, item_id = {item_id}, item_quantity = {item_quantity}, item_amount = {item_amount}")
                
                if item_id is not None:
                    values_for_hk_order_service_item.append((
                        hk_order_id,
                        item_id,
                        item_quantity,
                        item_amount
                    ))

            resolved_time_formatted = None
            try:
                timestamp = int(created_at)
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                logging.error(f"Invalid 'createdAt' {created_at}. Skipping this record.")
            try:
                if resolved_time:
                    timestamp = int(resolved_time)
                    resolved_time_formatted = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    logging.error(f"Invalid or missing 'resolved_time'")
            except (ValueError, TypeError):
                logging.error(f"Invalid 'resolved_time' {resolved_time}.")
            
            values_for_hk_order.append((
                hk_order_id,
                room_no,
                resolved_time_formatted,
                date,
                hotel_id,
                booking_id
            ))

        logging.info(f"All data transformed. Inserting into postgres...\n\n")

        if values_for_hk_order:
            insert_query_hk_order_table = """
            INSERT INTO test_analytics.housekeeping_order (
                    id, room_no, resolved_at, created_at, hotel_id, hotel_booking_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_hk_order_table, values_for_hk_order)
            conn.commit()
        
        logging.info(f"values for hk order service item: {values_for_hk_order_service_item}\n\n\n")

        if values_for_hk_order_service_item:
            insert_query_hk_order_service_item_table = """
                INSERT INTO test_analytics.housekeeping_order_service_item (
                    housekeeping_order_id, housekeeping_service_item_id, count, amount
                )
                VALUES %s ON CONFLICT(housekeeping_order_id, housekeeping_service_item_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_hk_order_service_item_table, values_for_hk_order_service_item)

        conn.commit()
        logging.info(f"Skipped {skipped_count_no_hotel} records due to missing hotel ID")
        logging.info(f"Skipped {skip_count_no_service_item_id} records due to missing item id.")
        logging.info(f"Inserted {len(values_for_hk_order)} values_for_hk_order records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_hk_order_service_item)} values for hk order service item records into PostgreSQL.")

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

with DAG('load_hk_order_pipeline', 
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
    
