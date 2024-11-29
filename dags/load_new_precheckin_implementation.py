from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2 import extras
import logging
from datetime import datetime
from dateutil import parser
from essentials import *
import uuid


def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = integrationsTable  
    fileName = 'new_precheckin_implementation.json'
    
    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData

    pk_value = "HOTEL"
    sk_value = "PRECHECKIN#"

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

        values_for_precheckin = []
        booking_upsell_values = []
        hotel_booking_values = []
        
        hotel_ids = fetch_ids('id', 'test_analytics.hotel')
        skip_count_no_hotel_id = 0
        
        skip_count_no_checkin_date = 0

        for dynamo_item in data:
            hotel_id = dynamo_item.get('HotelId', {}).get('S', '')
            checkin_room = dynamo_item.get('checkInRoom', {}).get('S', '')
            room_type = dynamo_item.get('RoomType', {}).get('S', '')
            checkin_date = convert_to_date(dynamo_item.get('CheckinDate', {}).get('S', ''))
            checkout_date = convert_to_date(dynamo_item.get('CheckoutDate', {}).get('S', ''))
            guest_count_data = dynamo_item.get('GuestCount', {}).get('M', {})
            adult_count = int(guest_count_data.get('Adult', {}).get('N', 0))
            children_count = int(guest_count_data.get('Children', {}).get('N', 0))
            numberOfGuests = adult_count + children_count
            reservationType = dynamo_item.get('ReservationType', {}).get('S', '')
            pmsStatus = 'CONFIRMED'
            channel = dynamo_item.get('Channel', {}).get('S', '')            
            booking_id = dynamo_item.get('BookingId', {}).get('S', '')
            created_at = dynamo_item.get('preCheckInTime', {}).get('N', 0)
            channel = dynamo_item.get('Channel', {}).get('S', '')
            upsells = dynamo_item.get('Upsell', {}).get('L', [])
            
            if checkin_date is None:
                logging.info(f"Checkin date is invalid {checkin_date}")
                store_skip_data_to_file(dynamo_item, 'checkin_date is none', 'skipped_new_pre_checkin_checkin_date_none.json')
                skip_count_no_checkin_date += 1
                continue
            
            if hotel_id not in hotel_ids:
                logging.info(f"Hotel id not in hotel table {hotel_id}")
                store_skip_data_to_file(dynamo_item, 'hotel_id not found', 'skipped_new_pre_checkin_hotel_id_not_found.json')
                skip_count_no_hotel_id += 1
                continue
            
            for upsell in upsells:
                upsell_data = upsell.get('M', {})
                
                upsell_name = upsell_data.get('UpsellName', {}).get('S', '')
                upsell_revenue = upsell_data.get('Revenue', {}).get('N', 0)
                upsell_revenue = int(upsell_revenue)
                upsell_id = 'upsell#'+ str(uuid.uuid1())
                
                upsell_values = (
                    upsell_id,
                    upsell_name,
                    upsell_revenue,
                    booking_id
                )
                booking_upsell_values.append(upsell_values)

            try:
                timestamp = int(created_at)
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                logging.error(f"Invalid 'preCheckInTime' timestamp for booking_id {booking_id}")
                
            values = (
                booking_id,
                reservationType,
                checkin_room,
                room_type,
                checkin_date,
                checkout_date,
                numberOfGuests,
                pmsStatus,
                False,
                date,
                hotel_id
            )
            hotel_booking_values.append(values)

            values_for_precheckin.append((
                booking_id,
                'Success',
                date,
                booking_id,
                channel
            ))

        if hotel_booking_values:
            insert_query_hotel_booking_table = """
                INSERT INTO test_analytics.hotel_booking (
                    id, type, room, room_type, check_in_date, check_out_date, number_of_guests, pms_status,
                    is_invited, created_at, hotel_id 
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_hotel_booking_table, hotel_booking_values)
            
        if values_for_precheckin:
            insert_query_booking_precheckin_table = """
                INSERT INTO test_analytics.booking_pre_check_in (
                    id, pre_checked_in_status, pre_checked_in_at, hotel_booking_id, channel
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_booking_precheckin_table, values_for_precheckin)
        
        if booking_upsell_values:
            insert_query_upsell = """
                INSERT INTO test_analytics.booking_upsell (
                    id, upsell_name, revenue, hotel_booking_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            extras.execute_values(cursor, insert_query_upsell, booking_upsell_values)

        conn.commit()
        logging.info(f"skipped {skip_count_no_hotel_id} records no hotel id in hotel table")
        logging.info(f"skipped {skip_count_no_checkin_date} records no valid checkin date")
        logging.info(f"Inserted {len(values_for_precheckin)} records into PostgreSQL.")

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


with DAG('load_precheckin_new_implementation_pipeline', 
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
