from fastapi import FastAPI, File, UploadFile,Form, BackgroundTasks, HTTPException, Depends
from fastapi.responses import JSONResponse
import pandas as pd
import os
import json
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Literal
from kafka import KafkaProducer
import hashlib
import shutil
from pydantic import BaseModel, Field, validator
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv

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
            os.path.join(logs_dir, "app.log"),
            when="midnight", 
            backupCount=7
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = os.environ.get("DATA_DIR", "./data")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
CURRENT_DIR = os.path.join(DATA_DIR, "current")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cdc-events")

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)
os.makedirs(CURRENT_DIR, exist_ok=True)

# Define supported table types
SUPPORTED_TABLE_TYPES = [
    "Asset", "Location", "WorkOrder", "PreventiveMaintenance", "PurchaseOrder"
]

app = FastAPI(title="Excel CDC Data Ingestion API")

# Initialize Kafka producer
def get_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None
        )
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return None

kafka_producer = get_kafka_producer()

# Pydantic models
class CDCEvent(BaseModel):
    event_id: str
    event_type: str  # insert, update, delete
    company_id: str
    table_name: str
    timestamp: str
    key_column: str
    key_value: str
    old_values: Optional[Dict[str, Any]] = Field(default=None)
    new_values: Optional[Dict[str, Any]] = Field(default=None)

class ProcessingResponse(BaseModel):
    message: str
    file_id: str
    company_id: str
    table_name: str
    changes_detected: int

class UploadRequest(BaseModel):
    company_id: str
    table_type: Literal["Asset", "Location", "WorkOrder", "PreventiveMaintenance", "PurchaseOrder"]
    
    @validator('table_type')
    def validate_table_type(cls, v):
        if v not in SUPPORTED_TABLE_TYPES:
            raise ValueError(f"Invalid table type. Must be one of: {SUPPORTED_TABLE_TYPES}")
        return v

# Helper functions
def generate_file_id(filename: str, content: bytes) -> str:
    """Generate a unique file ID based on filename and content hash"""
    content_hash = hashlib.md5(content).hexdigest()
    return f"{os.path.splitext(filename)[0]}_{content_hash[:8]}"

def get_company_id_from_filename(filename: str) -> str:
    """Extract company ID from filename"""
    # Assuming filename format is like "Company_X_Data.csv"
    return os.path.splitext(filename)[0].replace(" ", "_")

def detect_changes(new_file_path: str, company_id: str, table_name: str) -> List[CDCEvent]:
    """Detect changes between the new file and the previous version"""
    logger.info(f"Detecting changes for company: {company_id}, table: {table_name}")
    
    # Find the most recent previous file for this company and table
    previous_files = [f for f in os.listdir(CURRENT_DIR) 
                     if f.startswith(f"{company_id}_{table_name}") and f.endswith(".csv")]
    
    # Read the new file
    try:
        new_df = pd.read_csv(new_file_path)
        
        # Handle null values properly
        new_df = new_df.astype(object).where(pd.notnull(new_df), None)
    except Exception as e:
        logger.error(f"Error reading new file: {e}")
        raise HTTPException(status_code=400, detail=f"Error reading CSV file: {str(e)}")
    
    # If no columns, raise error
    if len(new_df.columns) == 0:
        raise HTTPException(status_code=400, detail="CSV file has no columns")
    
    # Assuming first column is the key column
    key_column = new_df.columns[0]
    logger.info(f"Using key column: {key_column}")
    
    # Initialize empty list for CDC events
    cdc_events = []
    
    # If no previous file exists, treat all rows as inserts
    if not previous_files:
        logger.info("No previous file found. Treating all rows as inserts.")
        
        for _, row in new_df.iterrows():
            event = CDCEvent(
                event_id=str(uuid.uuid4()),
                event_type="insert",
                company_id=company_id,
                table_name=table_name,
                timestamp=datetime.now().isoformat(),
                key_column=key_column,
                key_value=str(row[key_column]),
                old_values=None,
                new_values=row.to_dict()
            )
            cdc_events.append(event)
        
        return cdc_events
    
    # Sort previous files by creation time (newest first)
    previous_files.sort(key=lambda x: os.path.getmtime(os.path.join(CURRENT_DIR, x)), reverse=True)
    previous_file_path = os.path.join(CURRENT_DIR, previous_files[0])
    
    try:
        prev_df = pd.read_csv(previous_file_path)
        
        # Handle null values properly
        prev_df = prev_df.astype(object).where(pd.notnull(prev_df), None)
    except Exception as e:
        logger.error(f"Error reading previous file: {e}")
        # If we can't read the previous file, treat all as inserts
        
        for _, row in new_df.iterrows():
            event = CDCEvent(
                event_id=str(uuid.uuid4()),
                event_type="insert",
                company_id=company_id,
                table_name=table_name,
                timestamp=datetime.now().isoformat(),
                key_column=key_column,
                key_value=str(row[key_column]),
                old_values=None,
                new_values=row.to_dict()
            )
            cdc_events.append(event)
        
        return cdc_events
    
    # Convert DataFrames to dictionaries for easier comparison
    # Using the first column as the key
    new_dict = {str(row[key_column]): row.to_dict() for _, row in new_df.iterrows()}
    prev_dict = {str(row[key_column]): row.to_dict() for _, row in prev_df.iterrows()}
    
    # Find inserts and updates
    for key, new_row in new_dict.items():
        if key not in prev_dict:
            # This is an insert
            event = CDCEvent(
                event_id=str(uuid.uuid4()),
                event_type="insert",
                company_id=company_id,
                table_name=table_name,
                timestamp=datetime.now().isoformat(),
                key_column=key_column,
                key_value=key,
                old_values=None,
                new_values=new_row
            )
            cdc_events.append(event)
        else:
            # Check if any values changed
            old_row = prev_dict[key]
            changes = {}
            
            for col in new_row:
                if col in old_row and new_row[col] != old_row[col]:
                    changes[col] = {"old": old_row[col], "new": new_row[col]}
            
            if changes:
                event = CDCEvent(
                    event_id=str(uuid.uuid4()),
                    event_type="update",
                    company_id=company_id,
                    table_name=table_name,
                    timestamp=datetime.now().isoformat(),
                    key_column=key_column,
                    key_value=key,
                    old_values=old_row,
                    new_values=new_row
                )
                cdc_events.append(event)
    
    # Find deletes
    for key, old_row in prev_dict.items():
        if key not in new_dict:
            event = CDCEvent(
                event_id=str(uuid.uuid4()),
                event_type="delete",
                company_id=company_id,
                table_name=table_name,
                timestamp=datetime.now().isoformat(),
                key_column=key_column,
                key_value=key,
                old_values=old_row,
                new_values=None
            )
            cdc_events.append(event)
    
    return cdc_events

def send_events_to_kafka(events: List[CDCEvent]):
    """Send CDC events to Kafka"""
    if not kafka_producer:
        logger.error("Kafka producer not available")
        return False
    
    try:
        for event in events:
            # Use company_id + table_name + event_type as the key for proper partitioning
            key = f"{event.company_id}_{event.table_name}_{event.event_type}"
            # Convert to dict for serialization
            event_dict = event.dict()
            # Send to Kafka
            kafka_producer.send(KAFKA_TOPIC, key=key, value=event_dict)
        
        # Ensure all messages are sent
        kafka_producer.flush()
        logger.info(f"Sent {len(events)} events to Kafka")
        return True
    except Exception as e:
        logger.error(f"Error sending events to Kafka: {e}")
        return False


async def process_file(file_path: str, company_id: str, table_name: str):
    """Process the uploaded file and send CDC events to Kafka"""
    try:
        # Detect changes
        cdc_events = detect_changes(file_path, company_id, table_name)
        
        # Send events to Kafka
        if cdc_events:
            success = send_events_to_kafka(cdc_events)
            if not success:
                logger.warning("Failed to send events to Kafka, but processing will continue")
        
        # Move file to current directory (replacing any existing file)
        filename = os.path.basename(file_path)
        current_file_path = os.path.join(CURRENT_DIR, f"{company_id}_{table_name}_{filename}")
        
        # Archive the current file if it exists
        if os.path.exists(current_file_path):
            archive_filename = f"{company_id}_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
            shutil.copy2(current_file_path, os.path.join(ARCHIVE_DIR, archive_filename))
        
        # Move new file to current directory
        shutil.copy2(file_path, current_file_path)
        
        return len(cdc_events)
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise e
    finally:
        # Clean up the temporary file
        if os.path.exists(file_path):
            os.remove(file_path)



@app.post("/upload", response_model=ProcessingResponse)
async def upload_file(
    background_tasks: BackgroundTasks,
    company_id: str = Form(...),
    table_type: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Upload a CSV file for CDC processing
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")
    
    # Read file content
    content = await file.read()
    
    # Generate file ID
    file_id = generate_file_id(file.filename, content)
    
    # Save file to temporary location
    temp_file_path = os.path.join(DATA_DIR, f"temp_{file_id}.csv")
    with open(temp_file_path, "wb") as f:
        f.write(content)
    
    try:
        # Process the file with all required parameters
        changes_count = await process_file(temp_file_path, company_id, table_type)
        
        return ProcessingResponse(
            message="File processed successfully",
            file_id=file_id,
            company_id=company_id,
            table_name=table_type,
            changes_detected=changes_count
        )
    except Exception as e:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")



@app.get("/health")
async def health_check():
    """Health check endpoint"""
    kafka_health = "UP" if kafka_producer else "DOWN"
    
    return {
        "status": "UP",
        "kafka": kafka_health,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

