import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any

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
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/cdc_data")
DELTA_DIR = os.environ.get("DELTA_DIR", "./data/delta")
PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "cdc-processor")

# Azure Storage settings
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "")
AZURE_STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "cdcdata")
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
    try:
        with conn.cursor() as cur:
            # Replace NaN with None in old_values and new_values
            old_values = json.dumps(replace_nan_with_none(event['old_values']), default=str) if event['old_values'] else None
            new_values = json.dumps(replace_nan_with_none(event['new_values']), default=str) if event['new_values'] else None

            cur.execute("""
                INSERT INTO cdc_events 
                (event_id, event_type, company_id, table_name, timestamp, key_column, key_value, old_values, new_values)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
            """, (
                event['event_id'],
                event['event_type'],
                event['company_id'],
                event['table_name'],
                event['timestamp'],
                event['key_column'],
                event['key_value'],
                old_values,
                new_values
            ))
            
            # If the event was already processed, skip it
            if cur.rowcount == 0:
                logger.info(f"Event {event['event_id']} already processed, skipping")
                return
            
            # Now handle the change in the company_data table
            if event['event_type'] == 'insert' or event['event_type'] == 'update':
                # For insert or update, we need to handle the history
                
                # First, mark any current record as not current
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
                    event['company_id'],
                    event['table_name'],
                    event['key_value']
                ))
                
                # Then insert the new record
                cur.execute("""
                    INSERT INTO company_data
                    (company_id, table_name, record_key, data, valid_from, is_current)
                    VALUES (%s, %s, %s, %s, %s, TRUE)
                """, (
                    event['company_id'],
                    event['table_name'],
                    event['key_value'],
                    json.dumps(replace_nan_with_none(event['new_values'])),
                    event['timestamp']
                ))
                
            elif event['event_type'] == 'delete':
                # For delete, mark the record as not current
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
                    event['company_id'],
                    event['table_name'],
                    event['key_value']
                ))
            
            # Mark the event as processed
            cur.execute("""
                UPDATE cdc_events
                SET processed = TRUE
                WHERE event_id = %s
            """, (event['event_id'],))
            
            conn.commit()
            logger.info(f"Processed event {event['event_id']} of type {event['event_type']}")
            
            # Generate delta files for this event
            generate_delta_table(event)
            
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        conn.rollback()

def generate_delta_table(event: Dict[str, Any]):
    """Generate a delta table for the event using Delta Lake"""
    try:
        # Create a directory structure for delta files
        company_id = event['company_id']
        table_name = event['table_name']
        
        # Determine if we're using local or Azure storage
        if USE_AZURE_STORAGE:
            logger.info("Azure Data Lake storage is enabled. Processing with Azure Data Lake.")
            
            # Generate DataFrame from event
            df = create_event_dataframe(event)
            
            # Upload to Azure Data Lake
            azure_manager = AzureDataLakeManager()
            if azure_manager.upload_cdc_event(event, df):
                logger.info(f"Successfully uploaded event to Azure Data Lake for {company_id}/{table_name}")
            else:
                logger.error(f"Failed to upload event to Azure Data Lake for {company_id}/{table_name}")
        else:
            logger.info("Azure Data Lake storage is disabled. Falling back to local Delta Lake processing.")
            
            # Use deltaprocessing to create Delta tables
            from deltaprocessing import create_enhanced_delta_table
            
            try:
                # Process with Delta Lake
                create_enhanced_delta_table(event)
                logger.info(f"Successfully created/updated Delta table for {company_id}/{table_name}")
            except Exception as delta_error:
                logger.error(f"Error with Delta processing: {delta_error}")
                
                # Fallback to parquet if Delta fails
                logger.info("Falling back to parquet storage")
                df = create_event_dataframe(event)
                
                # Local storage path for parquet fallback
                company_dir = os.path.join(PARQUET_DIR, company_id)
                table_dir = os.path.join(company_dir, table_name)
                os.makedirs(table_dir, exist_ok=True)
                
                # Save as parquet with timestamp
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                file_path = os.path.join(table_dir, f"{event['key_value']}_{timestamp}.parquet")
                df.to_parquet(file_path, index=False)
                logger.info(f"Saved event data to parquet: {file_path}")
        
    except Exception as e:
        logger.error(f"Error generating delta table: {e}")

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
    
    # Create a column for all keys from new_values
    if event['new_values']:
        for key, value in event['new_values'].items():
            cleaned_value = replace_nan_with_none(value)
            data[key] = [cleaned_value]  # Add individual columns for each field
    
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