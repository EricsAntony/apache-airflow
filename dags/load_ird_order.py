from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime
from essentials import *
from psycopg2 import extras

def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = propertyTable
    fileName = 'ird_order.json'

    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} ird order data from {fileName}")
        return extractedData
    
    pk_value = "HOTEL#"
    sk_value = "ORDER#"

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

        values_for_ird_order = []
        values_for_ird_order_items = []
        values_for_ird_order_addon = []
        values_for_ird_order_customisation = []
        
        hotel_ids = fetch_ids('id', 'test_analytics.hotel')
        skip_count = 0
        
        ird_item_codes = fetch_ids('code', 'test_analytics.ird_item')
        skip_count_no_ird_item_code = 0
        
        order_ids = fetch_ids('id', 'test_analytics.ird_order')
        
        addon_codes = fetch_ids('code', 'test_analytics.ird_item_addon')
        skip_count_no_addon_code = 0
        
        customisation_codes = fetch_ids('code', 'test_analytics.ird_item_customisation')
        skip_count_no_customisation_code = 0
        
        filtered_dynamo_items_first_layer = []
        filtered_dynamo_items_second_layer = []
        
        query = 'SELECT id, code, hotel_id FROM test_analytics.ird_item'
        item_result = []
        skip_count_no_item_id = 0
        
        query_addon = 'SELECT id, code, hotel_id FROM test_analytics.ird_item_addon'
        addon_result = []
        skip_count_no_addon_id = 0
        
        query_customisation = 'SELECT id, code, hotel_id FROM test_analytics.ird_item_customisation'
        customisation_result = []
        skip_count_no_customisation_id = 0
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            
            cursor.execute(query_addon)
            addon_result = cursor.fetchall()
            
            cursor.execute(query_customisation)
            customisation_result = cursor.fetchall()
        except Exception as e:
            logging.info(f"error: {e}")
            raise
                
        for dynamo_item in data:
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            if hotel_id not in hotel_ids:
                logging.warning(f"Skipping item due to invalid hotel ID {hotel_id} in hotel table.")
                skip_count += 1
                store_skip_data_to_file(dynamo_item, 'no hotel in hotel table', 'skipped_ird_order_invalid_hotel_id.json')
                continue
            filtered_dynamo_items_first_layer.append(dynamo_item)
        
        for dynamo_item in filtered_dynamo_items_first_layer:
            appendFlag = True
            items = dynamo_item.get('items', {}).get('L', [])

            for item in items:
                itemMap = item.get('M', {})
                
                addOns = itemMap.get('addOns', {}).get('L', [])
                item_code = itemMap.get('code', {}).get('S', '')
                customisations = itemMap.get('customisations', {}).get('L', [])
                
                
                if item_code not in ird_item_codes:
                    logging.warning(f"Skipping item with code {item_code} due to invalid item ID in ird item table.")
                    skip_count_no_ird_item_code += 1
                    store_skip_data_to_file(dynamo_item, 'item id not in ird item table', 'skipped_ird_order_no_item_id.json')
                    appendFlag = False
                    continue
                
                for addOn in addOns:
                    addOnMap = addOn.get('M', {})
                    addOnCode = addOnMap.get('code', {}).get('S', '')
                    
                    if addOnCode not in addon_codes:
                        logging.warning(f"Skipping add-on with code {addOnCode} due to invalid add-on ID in ird item addon table.")
                        skip_count_no_addon_code += 1
                        store_skip_data_to_file(dynamo_item, 'addon id not in ird addon table', 'skipped_ird_order_no_addon_id.json')
                        appendFlag = False
                        continue
                                    
                for customisation in customisations:
                    customisationMap = customisation.get('M', {})
                    customisationCode = customisationMap.get('code', {}).get('S', '')
                    
                    if customisationCode not in customisation_codes:
                        logging.warning(f"Skipping customisation with code {customisationCode} due to invalid customisation ID in ird item customisation table.")
                        skip_count_no_customisation_code += 1
                        store_skip_data_to_file(dynamo_item, 'customisation id not in ird item table', 'skipped_ird_order_no_customisation_id.json')
                        appendFlag = False
                        continue
                    
            if appendFlag:
                filtered_dynamo_items_second_layer.append(dynamo_item)
                order_ids.append(dynamo_item.get('id', {}.get('S', '')))

                
        logging.info(f"final dynamo_item: {len(filtered_dynamo_items_second_layer)}")
        
                    
        for dynamo_item in filtered_dynamo_items_second_layer:
            order_id = dynamo_item.get('id', {}).get('S', '')
            room_no = dynamo_item.get('room_no', {}).get('S', '')
            start_at = parse_timestamp(dynamo_item.get('startTime', {}).get('S', ''))
            completed_at = parse_timestamp(dynamo_item.get('completedTime', {}).get('S', ''))
            total_amount = dynamo_item.get('totalAmount', {}).get('N', 0)
            payment_method = dynamo_item.get('paymentMethod', {}).get('S', '')
            no_of_guests = dynamo_item.get('noOfGuests', {}).get('N', 0)
            created_at = dynamo_item.get('createdAt', {}).get('N', 0)
            hotel_id = dynamo_item.get('hotelId', {}).get('S', '')
            booking_id = dynamo_item.get('bookingId', {}).get('S', '')
            items = dynamo_item.get('items', {}).get('L', [])
            
            for item in items:
                itemMap = item.get('M', {})
                
                addOns = itemMap.get('addOns', {}).get('L', [])
                amount = itemMap.get('amount', {}).get('N', 0) 
                count = itemMap.get('count', {}).get('N', 0)
                item_code = itemMap.get('code', {}).get('S', '')
                customisations = itemMap.get('customisations', {}).get('L', [])
                isUpsell = itemMap.get('isUpsell', {}).get('BOOL', False)
                item_id = None
                
                logging.info(f"Processing item with amount: {amount}, count: {count}, isUpsell: {isUpsell}")
                
                for row in result:
                    if row[1] == item_code and row[2] == hotel_id:
                        item_id = row[0]
                        break                        
                
                if item_id is None or str.isspace(item_id):
                    logging.warning(f"Skipping item with code {item_code} due to invalid item ID in ird item table. Hotel id: {hotel_id}. {item_id}")
                    skip_count_no_item_id += 1
                    store_skip_data_to_file(dynamo_item, 'no invalid item Id in table')
                    continue
                
                values_for_ird_order_items.append((
                    order_id,
                    item_id,
                    count,
                    amount,
                    isUpsell,
                    1
                ))
                
                for addOn in addOns:
                    addOnMap = addOn.get('M', {})
                    addOnCode = addOnMap.get('code', {}).get('S', '')
                    addOnName = addOnMap.get('name', {}).get('S', '')
                    addOnPrice = addOnMap.get('price', {}).get('N', 0)
                    addOn_id = None
                    
                    logging.info(f"AddOn: {addOnName}, Code: {addOnCode}, Price: {addOnPrice}")
                    
                    for row in addon_result:
                        if row[1] == addOnCode and row[2] == hotel_id:
                            addOn_id = row[0]
                            break                        
                
                    if addOn_id is None or str.isspace(addOn_id):
                        logging.warning(f"Skipping item with code {addOnCode} due to invalid addon id in ird item table.Hotel id: {hotel_id}. {addOn_id}")
                        skip_count_no_addon_id += 1
                        store_skip_data_to_file(dynamo_item, 'no invalid addon Id in table')
                        continue
                    
                    values_for_ird_order_addon.append((
                        order_id,
                        item_id,
                        addOn_id                                                               
                        ))
                    
                
                for customisation in customisations:
                    customisationMap = customisation.get('M', {})
                    customisationCode = customisationMap.get('code', {}).get('S', '')
                    customisationName = customisationMap.get('name', {}).get('S', '')
                    customisation_id = None
                    logging.info(f"Customisation: {customisationName}, Code: {customisationCode}")
                    
                    for row in customisation_result:
                        if row[1] == customisationCode and row[2] == hotel_id:
                            customisation_id = row[0]
                            break                        
                
                    if customisation_id is None or str.isspace(customisation_id):
                        logging.warning(f"Skipping item with code {customisationCode} due to invalid customisation id in ird item table. Hotel id: {hotel_id}. {customisation_id}")
                        skip_count_no_customisation_id += 1
                        store_skip_data_to_file(dynamo_item, 'no invalid customisation id in table')
                        continue

                    values_for_ird_order_customisation.append((
                        order_id,
                        item_id,
                        customisation_id                                                               
                        ))
                
            try:
                timestamp = int(created_at)
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                logging.error(f"Invalid 'CheckOutTime' timestamp for booking_id {booking_id}.")
                
            values_for_ird_order.append((
                order_id,
                room_no,
                start_at,
                completed_at,
                total_amount,
                payment_method,
                no_of_guests,
                date,
                hotel_id,
                booking_id
            ))

        if values_for_ird_order:
            insert_query_ird_order_table = """
                INSERT INTO test_analytics.ird_order (
                    id, room_no, start_at, completed_at, total_amount, payment_method, no_of_guests,created_at, hotel_id, hotel_booking_id
                )
                VALUES %s ON CONFLICT(id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_ird_order_table, values_for_ird_order)
            conn.commit()
            
        if values_for_ird_order_items:
            insert_query_ird_order_item_table = """
            INSERT INTO test_analytics.ird_order_item (
                    ird_order_id, ird_item_id, count, amount, is_upsell, ird_menu_id
                )
                VALUES %s ON CONFLICT (ird_order_id, ird_item_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_ird_order_item_table, values_for_ird_order_items)
            conn.commit()
            
        if values_for_ird_order_addon:
            insert_query_ird_order_addon_table = """
                INSERT INTO test_analytics.ird_order_item_addon (
                    ird_order_id, ird_item_id, ird_item_addon_id
                )
                VALUES %s ON CONFLICT (ird_order_id, ird_item_id, ird_item_addon_id) DO NOTHING; 
            """
            extras.execute_values(cursor, insert_query_ird_order_addon_table, values_for_ird_order_addon)
            conn.commit()
            
        if values_for_ird_order_customisation:
            insert_query_ird_order_customisation_table = """
                INSERT INTO test_analytics.ird_order_item_customisation (
                    ird_order_id, ird_item_id, ird_item_customisation_id
                )
                VALUES %s ON CONFLICT (ird_order_id, ird_item_id, ird_item_customisation_id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_ird_order_customisation_table, values_for_ird_order_customisation)
            conn.commit()
            
        conn.commit()
        logging.info(f"Skipped {skip_count} orders because hotel id not found in hotel table")
        logging.info(f"skipped {skip_count_no_item_id} records, invalid item id in ird item table.")
        logging.info(f"Skipped {skip_count_no_addon_id} records, invalid addon id in ird addon table.")
        logging.info(f"Skipped {skip_count_no_customisation_id} records, invalid customisation id in ird item customisation table.")
        logging.info(f"Skipped {skip_count_no_ird_item_code} items because invalid item code in ird item table")
        logging.info(f"Skipped {skip_count_no_addon_code} items because invalid addon code in ird item addon table")
        logging.info(f"Skipped {skip_count_no_customisation_code} items because invalid customisation code in ird item customisation table.")
        logging.info(f"Inserted {len(values_for_ird_order)} ird order records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_ird_order_items)} ird item records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_ird_order_addon)} addon records into PostgreSQL.")
        logging.info(f"Inserted {len(values_for_ird_order_customisation)} customisation records into PostgreSQL.")

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

with DAG('load_ird_order_pipeline', 
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
    
