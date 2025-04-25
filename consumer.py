import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any
import threading

import pandas as pd
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
from deltaprocessing import *
import pyarrow as pa
# from delta import *
from delta.tables import *
from delta.tables import DeltaTable
import pyarrow.parquet as pq
from logging.handlers import TimedRotatingFileHandler
from azure.storage.filedatalake import DataLakeServiceClient
from azuredatalake import AzureDataLakeManager
from automl_anomaly_detection import run_anomaly_detection_for_event

# Load environment variables
load_dotenv()

# Set up logging
logs_dir = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        TimedRotatingFileHandler(
            os.path.join(logs_dir, "consumer.log"),
            when="midnight",  # Rotate logs daily
            backupCount=7      # Keep 7 days of logs
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cdc-events")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", 
    "postgresql://adil:admin123@20.9.138.28:5432/cdc_data"
)
DELTA_DIR = os.environ.get("DELTA_DIR", "./data/delta")
PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "cdc-processor")
print(CONSUMER_GROUP)

# Azure Storage settings
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "")
AZURE_STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "newcdcdata")
USE_AZURE_STORAGE = AZURE_STORAGE_CONNECTION_STRING != ""

# Create directories
os.makedirs(DELTA_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def get_azure_datalake_client():
    """Get Azure Data Lake Storage client"""
    if not USE_AZURE_STORAGE:
        return None
        
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=AZURE_STORAGE_ACCOUNT_KEY
        )
        return service_client
    except Exception as e:
        logger.error(f"Error connecting to Azure Data Lake: {e}")
        return None

def replace_nan_with_none(data):
    """Recursively replace NaN values with None in dictionaries and lists."""
    if isinstance(data, dict):
        return {k: replace_nan_with_none(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_nan_with_none(v) for v in data]
    elif pd.isna(data):  # Check for NaN
        return None
    else:
        return data

def process_event(event: Dict[str, Any], conn):
    """Processes CDC events with data validation and new structure"""
    try:
        # Validate company data exists
        if not event.get('company_id') or not event.get('table_name'):
            logger.error("Missing company_id or table_name in event")
            return False

        company_id = event['company_id'].strip().upper()
        table_name = event['table_name']
        event_type = event['event_type']

        logger.info(f"Processing {event_type} for {company_id}/{table_name}")

        # Verify we have actual data values
        if event_type in ['insert', 'update'] and not event.get('new_values'):
            logger.error(f"No new_values in {event_type} event")
            return False
        elif event_type == 'delete' and not event.get('old_values'):
            logger.error("No old_values in delete event")
            return False

        with conn.cursor() as cur:
            # 1. Store raw event
            cur.execute("""
                INSERT INTO cdc_events 
                (event_id, event_type, company_id, table_name, timestamp, 
                 key_column, key_value, old_values, new_values)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
            """, (
                event['event_id'],
                event_type,
                company_id,
                table_name,
                event['timestamp'],
                event['key_column'],
                str(event['key_value']),
                json.dumps(event['old_values']) if event.get('old_values') else None,
                json.dumps(event['new_values']) if event.get('new_values') else None
            ))

            if cur.rowcount == 0:
                logger.info(f"Event {event['event_id']} already processed")
                return True

            # 2. Update company data table
            if event_type in ['insert', 'update']:
                # Expire old version
                cur.execute("""
                    UPDATE company_data
                    SET is_current = FALSE, 
                        valid_to = %s
                    WHERE company_id = %s 
                      AND table_name = %s 
                      AND record_key = %s 
                      AND is_current = TRUE
                """, (
                    event['timestamp'],
                    company_id,
                    table_name,
                    str(event['key_value'])
                ))

                # Insert new version
                cur.execute("""
                    INSERT INTO company_data
                    (company_id, table_name, record_key, data, valid_from, is_current)
                    VALUES (%s, %s, %s, %s, %s, TRUE)
                """, (
                    company_id,
                    table_name,
                    str(event['key_value']),
                    json.dumps(event['new_values']),
                    event['timestamp']
                ))

            elif event_type == 'delete':
                # Mark as expired
                cur.execute("""
                    UPDATE company_data
                    SET is_current = FALSE, 
                        valid_to = %s
                    WHERE company_id = %s 
                      AND table_name = %s 
                      AND record_key = %s 
                      AND is_current = TRUE
                """, (
                    event['timestamp'],
                    company_id,
                    table_name,
                    str(event['key_value'])
                ))

            # 3. Mark event as processed
            cur.execute("""
                UPDATE cdc_events
                SET processed = TRUE
                WHERE event_id = %s
            """, (event['event_id'],))

            conn.commit()

            # 4. Generate delta files (async to avoid blocking)
            threading.Thread(
                target=generate_delta_table,
                args=(event,),
                daemon=True
            ).start()

            # 5. Trigger anomaly detection (async)
            threading.Thread(
                target=run_anomaly_detection_for_event,
                args=(event,),
                daemon=True
            ).start()

            return True

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        if conn:
            conn.rollback()
        raise e


# def generate_delta_table(event: Dict[str, Any]):
#     try:
#         company_id = event['company_id']  # "AEP"
#         table_name = event['table_name']  # "d_financials_sheet"
        
#         azure = AzureDataLakeManager()
#         df = create_event_dataframe(event)
        
#         # This now uses the client's preferred structure
#         azure.create_delta_table(event, df)
#     except Exception as e:
#         logger.error(f"Delta processing error: {str(e)}")

def generate_delta_table(event: Dict[str, Any]):
    try:
        company_id = event['company_id']
        df = create_event_dataframe(event)
        
        azure = AzureDataLakeManager()
        # Use the simplified upload method instead of Delta tables
        success = azure.upload_consolidated_data(company_id, df)
        
        if not success:
            logger.error("Failed to upload consolidated data to Azure")
        
    except Exception as e:
        logger.error(f"Data upload error: {str(e)}")

def create_event_dataframe(event: Dict[str, Any]):
    """Create a DataFrame from the event data"""
    # Create a DataFrame from the event
    data = {
        'event_id': [event['event_id']],
        'event_type': [event['event_type']],
        'company_id': [event['company_id']],
        'table_name': [event['table_name']],
        'timestamp': [event['timestamp']],
        'key_column': [event['key_column']],
        'key_value': [event['key_value']]
    }
    
    # Store the entire payload as JSON
    data['data'] = [json.dumps(replace_nan_with_none(event['new_values'] if event['new_values'] else event['old_values']), default=str)]
    
    # Look for date columns in the event data
    date_column = None
    date_column_value = None
    
    # First check if the key_column is a date column
    if event['key_column'].lower() in ["date", "time", "period", "month", "day"]:
        date_column = event['key_column']
        date_column_value = event['key_value']
    
    # Otherwise look for date columns in the event data
    elif event['new_values']:
        for key in ["Date", "date", "TIME", "time", "Period", "period", "MONTH", "month", "DAY", "day"]:
            if key in event['new_values']:
                date_column = key
                date_column_value = event['new_values'][key]
                break
        
        # If no date column found, use the first column
        if not date_column and event['new_values']:
            date_column = list(event['new_values'].keys())[0]
            date_column_value = event['new_values'][date_column]
    
    # Make sure we have a date column
    if not date_column:
        date_column = "Date"
        date_column_value = datetime.now().strftime("%Y-%m-%d")
    
    # Add the date column to our data
    data[date_column] = [date_column_value]
    
    # Now add other values from new_values, but SKIP the date column that we already added
    if event['new_values']:
        for key, value in event['new_values'].items():
            # Skip if this is our date column (already added) or already exists
            if key == date_column or key in data:
                continue
                
            cleaned_value = replace_nan_with_none(value)
            data[key] = [cleaned_value]
    
    # Add operational data marker for joining
    data['_operational'] = [True]
    data['_date_column'] = [date_column]
    
    return pd.DataFrame(data)

def upload_to_azure_datalake(df, company_id, table_name, event):
    """Upload data to Azure Data Lake in Delta format"""
    try:
        # Get Azure Data Lake client
        service_client = get_azure_datalake_client()
        if not service_client:
            logger.error("Azure Data Lake client not available")
            return
        
        # Create filesystem client (container)
        try:
            file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER_NAME)
        except Exception:
            # Container doesn't exist, create it
            file_system_client = service_client.create_file_system(file_system=AZURE_CONTAINER_NAME)
        
        # Create directory structure
        directory_path = f"{company_id}/{table_name}"
        try:
            directory_client = file_system_client.get_directory_client(directory_path)
        except Exception:
            # Directory doesn't exist, create it
            directory_client = file_system_client.create_directory(directory_path)
        
        # Create a temporary local file for the data
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"{event['event_id']}.parquet")
        
        # Save DataFrame to parquet
        df.to_parquet(temp_file)
        
        # Upload the file to Azure
        file_name = f"{event['event_type']}_{event['key_value']}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        file_client = directory_client.create_file(file_name)
        
        with open(temp_file, "rb") as file_data:
            file_client.upload_data(file_data, overwrite=True)
        
        # Clean up temporary file
        os.remove(temp_file)
        
        logger.info(f"Uploaded file to Azure Data Lake: {directory_path}/{file_name}")
    except Exception as e:
        logger.error(f"Error uploading to Azure Data Lake: {e}")

def main():
    """Main function to consume CDC events from Kafka"""
    try:
        # Get database connection
        conn = get_db_connection()
        if not conn:
            logger.error("Could not connect to database, exiting")
            sys.exit(1)
        
        # Create Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        logger.info(f"Connected to Kafka, consuming from topic: {KAFKA_TOPIC}")
        
        # Consume messages
        for message in consumer:
            try:
                event = message.value
                logger.info(f"Received event: {event['event_id']} of type {event['event_type']}")
                process_event(event, conn)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Sleep briefly to avoid hammering the system in case of persistent errors
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Consumer shutting down")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed")
        if 'consumer' in locals() and consumer:
            consumer.close()
            logger.info("Kafka consumer closed")

if __name__ == "__main__":
    main()