# File: app/consumer.py
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
import pyarrow as pa
import pyarrow.parquet as pq
from logging.handlers import TimedRotatingFileHandler

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
PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "cdc-processor")

# Create directories
os.makedirs(PARQUET_DIR, exist_ok=True)

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

import pandas as pd

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
            
            # Generate parquet file for this event batch
            generate_parquet_file(event)
            
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        conn.rollback()

def generate_parquet_file(event: Dict[str, Any]):
    """Generate a parquet file for the event"""
    try:
        # Create a directory structure for parquet files
        company_dir = os.path.join(PARQUET_DIR, event['company_id'])
        table_dir = os.path.join(company_dir, event['table_name'])
        os.makedirs(table_dir, exist_ok=True)
        
        # Format timestamp for filename
        timestamp = datetime.fromisoformat(event['timestamp']).strftime('%Y%m%d%H%M%S')
        
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
        
        # Add old and new values if they exist
        if event['old_values']:
            for key, value in event['old_values'].items():
                data[f'old_{key}'] = [replace_nan_with_none(value)]  # Replace NaN with None
        
        if event['new_values']:
            for key, value in event['new_values'].items():
                data[f'new_{key}'] = [replace_nan_with_none(value)]  # Replace NaN with None
        
        df = pd.DataFrame(data)
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet
        parquet_file = os.path.join(table_dir, f"{event['event_type']}_{timestamp}_{event['key_value']}.parquet")
        pq.write_table(table, parquet_file)
        
        logger.info(f"Generated parquet file: {parquet_file}")
    except Exception as e:
        logger.error(f"Error generating parquet file: {e}")

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


