# from chatbot import router as chatbot_router
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
import zipfile

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
        
        # Extract the first column name (presumably "Date")
        date_column = new_df.columns[0] if len(new_df.columns) > 0 else "Date"
        
        # Handle null values properly
        new_df = new_df.astype(object).where(pd.notnull(new_df), None)
    except Exception as e:
        logger.error(f"Error reading new file: {e}")
        raise HTTPException(status_code=400, detail=f"Error reading CSV file: {str(e)}")
    
    # If no columns, raise error
    if len(new_df.columns) == 0:
        raise HTTPException(status_code=400, detail="CSV file has no columns")
    
    # Assuming first column is the key column (Date)
    key_column = new_df.columns[0]
    logger.info(f"Using key column: {key_column}")
    
    # Store the date column name for later use in creating parquet files
    with open(f"{new_file_path}.colname", "w") as f:
        f.write(key_column)
    
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
        # 1. First read the original file for debugging
        original_df = pd.read_csv(file_path)
        print("\n=== ORIGINAL FILE DEBUG ===")
        print("Columns:", original_df.columns.tolist())
        print("First 5 rows:\n", original_df.head())
        print("==========================\n")

        # 2. Detect changes (existing CDC logic)
        cdc_events = detect_changes(file_path, company_id, table_name)
        
        # 3. Send events to Kafka
        if cdc_events:
            success = send_events_to_kafka(cdc_events)
            if not success:
                logger.warning("Failed to send events to Kafka, but processing will continue")

        # 4. Process the original file for delta table creation
        azure = AzureDataLakeManager()
        azure.process_and_upload_data({
            'company_id': company_id,
            'table_name': table_name,
            'event_type': 'upload',
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat()
        }, original_df)

        # 5. File management (existing logic)
        filename = os.path.basename(file_path)
        current_file_path = os.path.join(CURRENT_DIR, f"{company_id}_{table_name}_{filename}")
        
        # Read the first column name that was saved during detect_changes
        first_column_name = "Date"  # Default value
        colname_file = f"{file_path}.colname"
        if os.path.exists(colname_file):
            with open(colname_file, "r") as f:
                first_column_name = f.read().strip()
        
        # Archive the current file if it exists
        if os.path.exists(current_file_path):
            archive_filename = f"{company_id}_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{filename}"
            shutil.copy2(current_file_path, os.path.join(ARCHIVE_DIR, archive_filename))
        
        # Move new file to current directory
        shutil.copy2(file_path, current_file_path)
        
        # Also store the first column name with the current file
        with open(f"{current_file_path}.colname", "w") as f:
            f.write(first_column_name)
        
        return len(cdc_events)
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise e
    finally:
        # Clean up the temporary files
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(f"{file_path}.colname"):
            os.remove(f"{file_path}.colname")


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
    if not (file.filename.endswith('.csv') or file.filename.endswith('.xlsx') or file.filename.endswith('.docx')):
        raise HTTPException(status_code=400, detail="Only CSV, XLSX, and DOCX files are accepted")
    
    # Read file content
    content = await file.read()
    
    # Generate file ID
    file_id = generate_file_id(file.filename, content)
    
    # Save file to temporary location
    temp_file_path = os.path.join(DATA_DIR, f"temp_{file_id}")
    
    # Convert files to CSV if needed
    if file.filename.endswith('.xlsx'):
        try:
            # Read Excel file
            excel_df = pd.read_excel(io.BytesIO(content))
            
            # DEBUG: Print columns and first few rows
            print("\n=== UPLOAD DEBUG START ===")
            print("File Type: Excel")
            print("All Columns:", list(excel_df.columns))
            print("First 3 Rows:")
            print(excel_df.head(10))
            print("=== UPLOAD DEBUG END ===\n")
            
            # Add .csv extension
            temp_file_path += '.csv'
            
            # Save as CSV
            excel_df.to_csv(temp_file_path, index=False)
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing Excel file: {str(e)}")
    elif file.filename.endswith('.docx'):
        try:
            # Convert Word to CSV
            csv_content = convert_docx_to_csv(content)
            
            # DEBUG: Print content
            print("\n=== UPLOAD DEBUG START ===")
            print("File Type: Word")
            print("Raw Content Preview:", csv_content[:500])  # First 500 chars
            print("=== UPLOAD DEBUG END ===\n")
            
            # Add .csv extension
            temp_file_path += '.csv'
            
            # Save as CSV
            with open(temp_file_path, "wb") as f:
                f.write(csv_content)
        except Exception as e:
            logger.error(f"Error processing Word file: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing Word file: {str(e)}")
    else:
        # For CSV files, save as is
        temp_file_path += '.csv'
        with open(temp_file_path, "wb") as f:
            f.write(content)
        
        # DEBUG: Print CSV content
        try:
            csv_df = pd.read_csv(temp_file_path)
            print("\n=== UPLOAD DEBUG START ===")
            print("File Type: CSV")
            print("All Columns:", list(csv_df.columns))
            print("First 3 Rows:")
            print(csv_df.head(3))
            print("=== UPLOAD DEBUG END ===\n")
        except Exception as e:
            print(f"Could not read CSV for debugging: {e}")
    
    try:
        # Read the file to extract column names BEFORE processing
        df = pd.read_csv(temp_file_path)
        first_column_name = df.columns[0] if len(df.columns) > 0 else "Date"
        
        # Add the first column name to the file path for reference
        with open(f"{temp_file_path}.colname", "w") as f:
            f.write(first_column_name)
        
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


@app.post("/upload-zip", response_model=ProcessingResponse)
async def upload_zip_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    output_folder: str = Form(".")
):
    """
    Process a ZIP file containing financial spreadsheets and create a single consolidated Parquet file
    in Azure Delta Lake with simple folder structure:
    {company_id}/consolidated_data.parquet
    """
    # Validate file type
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Only ZIP files are accepted")

    # Initialize variables to ensure cleanup works
    temp_zip_path = None
    
    try:
        # Save the zip file temporarily
        temp_zip_path = os.path.join(DATA_DIR, f"temp_{uuid.uuid4()}.zip")
        with open(temp_zip_path, "wb") as f:
            f.write(await file.read())

        # Process the zip file and get combined DataFrame
        combined_df = process_financial_zip(temp_zip_path, output_folder)

        # Extract company ID from filename (before first hyphen)
        company_id = os.path.splitext(file.filename)[0].split("-")[0].upper()
        
        # Initialize Azure Data Lake Manager
        azure = AzureDataLakeManager()
        
        # Upload consolidated data - YAHAN MAIN FUNCTION KO CALL KAR RAHA HOON
        success = azure.upload_consolidated_data(company_id, combined_df)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to upload consolidated data to Azure")
        
        return ProcessingResponse(
            message="ZIP file processed successfully. Data consolidated into single Parquet file.",
            file_id=str(uuid.uuid4()),
            company_id=company_id,
            table_name="consolidated_data",
            changes_detected=len(combined_df)
        )

    except ValueError as e:
        if "No valid data found" in str(e):
            raise HTTPException(
                status_code=400,
                detail="The ZIP file doesn't contain any valid Excel spreadsheets or the sheets are empty"
            )
        raise HTTPException(status_code=500, detail=f"Error processing ZIP file: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing ZIP file: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing ZIP file: {str(e)}")
    finally:
        # Clean up temporary files
        if temp_zip_path and os.path.exists(temp_zip_path):
            try:
                os.remove(temp_zip_path)
            except Exception as e:
                logger.error(f"Error cleaning up temp file {temp_zip_path}: {e}")
                                
def process_financial_zip(zip_path: str, output_folder: str) -> pd.DataFrame:
    """Process ZIP file with subfolder structure"""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get all XLSX files including subfolders
        xlsx_files = [f for f in zip_ref.namelist() 
                     if f.lower().endswith('.xlsx') and not f.startswith('__MACOSX')]
        
        if not xlsx_files:
            raise ValueError("No XLSX files found")

        extract_path = os.path.join(DATA_DIR, f"unzipped_{uuid.uuid4()}")
        zip_ref.extractall(extract_path)
        logger.info(f"Extracted {len(xlsx_files)} files")

    wide_records = []

    for root, _, files in os.walk(extract_path):
        for file in files:
            if not file.lower().endswith('.xlsx'):
                continue
                
            filepath = os.path.join(root, file)
            try:
                xls = pd.ExcelFile(filepath)
                logger.info(f"Processing {file} with {len(xls.sheet_names)} sheets")

                for sheet in xls.sheet_names:
                    try:
                        # More flexible reading
                        df = pd.read_excel(xls, sheet_name=sheet, header=None)
                        
                        # Skip if empty
                        if df.empty or df.shape[1] < 2:
                            continue

                        # Auto-detect header row
                        header_row = 0
                        for i in range(min(3, len(df))):  # Check first 3 rows
                            if any("date" in str(cell).lower() for cell in df.iloc[i]):
                                header_row = i
                                break
                                
                        headers = df.iloc[header_row].fillna("").astype(str).str.strip().tolist()
                        headers[0] = "METRIC"
                        df.columns = headers
                        df = df.iloc[header_row+1:].reset_index(drop=True)

                        # Flexible metadata extraction
                        company = os.path.splitext(file)[0].split("-")[0].upper().strip()
                        statement = sheet.split("-")[0].strip() if "-" in sheet else "GENERAL"
                        frequency = sheet.split("-")[-1].strip() if "-" in sheet else "ANNUAL"

                        df.insert(0, "SECTOR", "GAS UTILITY")
                        df.insert(1, "COMPANY", company)
                        df.insert(2, "STATEMENT", statement)
                        df.insert(3, "FREQUENCY", frequency)

                        wide_records.append(df)

                    except Exception as e:
                        logger.error(f"Sheet {sheet} error: {str(e)}")
                        continue

            except Exception as e:
                logger.error(f"File {file} error: {str(e)}")
                continue

    shutil.rmtree(extract_path, ignore_errors=True)
    
    if not wide_records:
        raise ValueError("No data found - check if sheets contain valid tables")
    
    return pd.concat(wide_records, ignore_index=True)

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

