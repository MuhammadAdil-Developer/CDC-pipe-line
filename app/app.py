from chatbot import router as chatbot_router
from fastapi import FastAPI, File, UploadFile,Form, BackgroundTasks, HTTPException, Query
from typing import List, Dict, Any, Optional, Literal
import pandas as pd
import mammoth
import io
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
from automl_anomaly_detection import AnomalyDetectionManager
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
app.include_router(chatbot_router, prefix="/api/v1")

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


def convert_docx_to_csv(docx_content: bytes) -> bytes:
    """
    Convert a .docx file to a CSV format
    
    Args:
        docx_content (bytes): Content of the Word document
    
    Returns:
        bytes: CSV content as bytes
    """
    try:
        # Convert .docx to HTML first
        result = mammoth.convert_to_html(io.BytesIO(docx_content))
        html_content = result.value

        # Attempt to convert HTML to a table-like structure
        # This is a simple conversion and might need customization based on specific document structures
        import pandas as pd
        from bs4 import BeautifulSoup

        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Try to find tables
        tables = soup.find_all('table')
        
        if tables:
            # Convert first table to DataFrame
            df = pd.read_html(str(tables[0]))[0]
        else:
            # If no tables, split by paragraphs
            paragraphs = soup.find_all('p')
            data = [p.get_text().split('\t') for p in paragraphs if p.get_text().strip()]
            df = pd.DataFrame(data)
        
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        return csv_buffer.getvalue().encode('utf-8')
    
    except Exception as e:
        logger.error(f"Error converting Word file to CSV: {e}")
        raise HTTPException(status_code=400, detail=f"Could not convert Word file to CSV: {str(e)}")


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
    Upload a CSV or XLSX file for CDC processing
    """
    # Validate file type
    if not (file.filename.endswith('.csv') or file.filename.endswith('.xlsx')):
        raise HTTPException(status_code=400, detail="Only CSV and XLSX files are accepted")
    
    # Read file content
    content = await file.read()
    
    # Generate file ID
    file_id = generate_file_id(file.filename, content)
    
    # Save file to temporary location
    temp_file_path = os.path.join(DATA_DIR, f"temp_{file_id}")
    
    # Convert XLSX to CSV if needed
    if file.filename.endswith('.xlsx'):
        try:
            # Read Excel file
            excel_df = pd.read_excel(io.BytesIO(content))
            
            # Add .csv extension
            temp_file_path += '.csv'
            
            # Save as CSV
            excel_df.to_csv(temp_file_path, index=False)
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing Excel file: {str(e)}")
    else:
        # For CSV files, save as is
        temp_file_path += '.csv'
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

@app.post("/anomaly/detect/{company_id}/{table_name}", response_model=Dict)
async def trigger_anomaly_detection(
    company_id: str,
    table_name: str,
    time_window_days: int = Query(30, description="Time window in days for analysis")
):
    """Trigger anomaly detection for a specific company and table"""
    try:
        detector = AnomalyDetectionManager()
        result = detector.detect_anomalies_in_company_data(
            company_id=company_id,
            table_name=table_name,
            metric_column="changes_count",
            time_window_days=time_window_days
        )
        
        return result
    except Exception as e:
        logger.error(f"Error triggering anomaly detection: {e}")
        raise HTTPException(status_code=500, detail=f"Error triggering anomaly detection: {str(e)}")

@app.get("/anomaly/results/{company_id}/{table_name}", response_model=List[Dict])
async def get_anomaly_results(
    company_id: str,
    table_name: str,
    limit: int = Query(10, description="Maximum number of results to return")
):
    """Get anomaly detection results for a specific company and table"""
    try:
        # Get the Azure Data Lake client
        azure_manager = AzureDataLakeManager()
        if not azure_manager or not azure_manager.file_system_client:
            raise HTTPException(status_code=500, detail="Azure Data Lake client not available")
        
        # Define the path for anomaly results
        anomaly_dir = f"analytics/{company_id}/{table_name}/anomalies"
        
        # List files in the directory
        try:
            directory_client = azure_manager.file_system_client.get_directory_client(anomaly_dir)
            paths = list(directory_client.get_paths())
        except Exception:
            # Directory doesn't exist or other error
            return []
        
        # Sort by last modified time (newest first)
        paths.sort(key=lambda p: p.last_modified, reverse=True)
        
        # Limit the number of results
        paths = paths[:limit]
        
        # Read the files and return the results
        results = []
        for path_item in paths:
            try:
                file_client = azure_manager.file_system_client.get_file_client(path_item.name)
                download = file_client.download_file()
                file_content = download.readall().decode('utf-8')
                result = json.loads(file_content)
                results.append(result)
            except Exception as e:
                logger.error(f"Error reading anomaly result file: {e}")
        
        return results
    except Exception as e:
        logger.error(f"Error getting anomaly results: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting anomaly results: {str(e)}")

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

