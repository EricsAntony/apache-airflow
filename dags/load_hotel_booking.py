from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import logging
from datetime import datetime
from essentials import *
import uuid


def pull_data_from_dynamodb(**kwargs):
    client = get_dynamodb_client()
    table_name = integrationsTable  
    fileName = 'hotel_booking.json'
    extractedData, haveData = extract_data_from_file(fileName, **kwargs)
    if haveData:
        logging.info(f"Got {len(extractedData)} from {fileName}")
        return extractedData

    pk_value = "HOTEL"
    sk_value = "CHECKIN#"

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

        hotel_booking_values = []
        booking_checkin_values = []
        booking_checkout_values = []
        booking_precheckin_values = []
        booking_upsell_values = []
        
        hotel_ids = fetch_ids('id', 'test_analytics.hotel')
        skip_count_no_hotel = 0
        
        booking_ids = fetch_ids('id', 'test_analytics.hotel_booking')
        skip_count_for_other_reasons = 0

        filtered_dynamo_items = []
        
        for dynamo_item in data:
            hotel_id = dynamo_item.get('HotelId', {}).get('S', '')
            booking_id = dynamo_item.get('BookingId', {}).get('S', '')
            checkin_date = convert_to_date(dynamo_item.get('CheckinDate', {}).get('S', ''))
            
            if hotel_id not in hotel_ids:
                logging.warning(f"Skipping related records for hotel id {hotel_id} as it does not exist in hotel table.")
                skip_count_no_hotel += 1
                store_skip_data_to_file(dynamo_item, 'Hotel id not exists in hotel table', 'skipped_hotel_booking.json')
                continue
            
            if booking_id != '' and str.isspace(booking_id) == False:
                if checkin_date is not None:
                    booking_ids.append(booking_id)
                    filtered_dynamo_items.append(dynamo_item)
                else:
                    logging.warning(f"Skipping related records for booking_id {booking_id} as it does not have valid checkin date.")
                    skip_count_for_other_reasons += 1
                    store_skip_data_to_file(dynamo_item, 'Checkin date is none', 'skipped_hotel_booking_checkin_date.json')
                    continue
            else:
                logging.warning(f"Skipping related records for booking_id {booking_id} as it does not have a booking ID.")
                skip_count_for_other_reasons += 1
                store_skip_data_to_file(dynamo_item, 'Invalid booking id', 'skipped_hotel_booking_invalid_bookingId.json')
                continue
                
        
        for dynamo_item in filtered_dynamo_items:
            hotel_id = dynamo_item.get('HotelId', {}).get('S', '')
            booking_id = dynamo_item.get('BookingId', {}).get('S', '')
            checkin_room = dynamo_item.get('checkInRoom', {}).get('S', '')
            room_type = dynamo_item.get('RoomType', {}).get('S', '')
            checkin_date = convert_to_date(dynamo_item.get('CheckinDate', {}).get('S', ''))
            checkout_date = convert_to_date(dynamo_item.get('CheckoutDate', {}).get('S', ''))
            guest_count_data = dynamo_item.get('GuestCount', {}).get('M', {})
            adult_count = int(guest_count_data.get('Adult', {}).get('S', '0'))
            children_count = int(guest_count_data.get('Children', {}).get('S', '0'))
            numberOfGuests = adult_count + children_count
            created_at = dynamo_item.get('createdAt', {}).get('N', 0)
            checkin_type = dynamo_item.get('CheckInType', {}).get('S', '')
            reservationType = dynamo_item.get('ReservationType', {}).get('S', '')
            pmsStatus = 'CONFIRMED'
            channel = dynamo_item.get('Channel', {}).get('S', '')
            upsells = dynamo_item.get('Upsell', {}).get('L', [])
            for upsell in upsells:
                upsell_data = upsell.get('M', {})
                
                upsell_name = upsell_data.get('UpsellName', {}).get('S', '')
                upsell_revenue = upsell_data.get('Revenue', {}).get('N', 0)
                upsell_revenue = upsell_revenue
                upsell_id = 'upsell#'+ str(uuid.uuid1())
                
                upsell_values = (
                    upsell_id,
                    upsell_name,
                    upsell_revenue,
                    booking_id
                )
                booking_upsell_values.append(upsell_values)
            
            try:
                if created_at == '0' or int(created_at) < 1:
                    raise ValueError("Invalid timestamp")

                timestamp = int(created_at)  
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            except (ValueError, OverflowError) as e:
                logging.error(f"Invalid 'createdAt' timestamp for booking_id {booking_id}: {e}")
                date = None 

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
           
            if checkin_type == 'CheckIn':
                checkin_values = (
                    booking_id,
                    'Success',
                    date,
                    channel,
                    booking_id
                )
                
                booking_checkin_values.append(checkin_values)

            if checkin_type == 'CheckOut':
                checkout_values = (
                    booking_id,
                    'Success',
                    date,
                    booking_id,
                    channel
                )
                booking_checkout_values.append(checkout_values)

            if checkin_type == 'PreCheckIn':
                precheckin_values = (
                    booking_id,
                    'Success',
                    date,
                    booking_id,
                    channel
                )
                booking_precheckin_values.append(precheckin_values)
                
        if hotel_booking_values:
            insert_query = """
                INSERT INTO test_analytics.hotel_booking (
                    id, type, room, room_type, check_in_date, check_out_date, number_of_guests, pms_status,
                    is_invited, created_at, hotel_id 
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            psycopg2.extras.execute_values(cursor, insert_query, hotel_booking_values)
                
        if booking_checkin_values:
            insert_query_checkin = """
                INSERT INTO test_analytics.booking_check_in (
                    id, check_in_status, checked_in_at, channel, hotel_booking_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            psycopg2.extras.execute_values(cursor, insert_query_checkin, booking_checkin_values)

        if booking_checkout_values:
            insert_query_checkout = """
                INSERT INTO test_analytics.booking_check_out (
                    id, check_out_status, checked_out_at, hotel_booking_id, channel
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            psycopg2.extras.execute_values(cursor, insert_query_checkout, booking_checkout_values)

        if booking_precheckin_values:
            insert_query_precheckin = """
                INSERT INTO test_analytics.booking_pre_check_in (
                    id, pre_checked_in_status, pre_checked_in_at, hotel_booking_id, channel
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            psycopg2.extras.execute_values(cursor, insert_query_precheckin, booking_precheckin_values)
            
        if booking_upsell_values:
            insert_query_upsell = """
                INSERT INTO test_analytics.booking_upsell (
                    id, upsell_name, revenue, hotel_booking_id
                )
                VALUES %s ON CONFLICT (id) DO NOTHING;
            """
            psycopg2.extras.execute_values(cursor, insert_query_upsell, booking_upsell_values)

        conn.commit()
        logging.info(f"skipped {skip_count_no_hotel} records because no hotel id found in hotel table")
        logging.info(f"skipped {skip_count_for_other_reasons} records because checkin date was not present or booking id is invalid")
        logging.info(f"Inserted {len(data)} records into PostgreSQL.")

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

with DAG('load_hotel_booking_pipeline', 
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
    
