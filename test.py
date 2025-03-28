# from azure.ai.textanalytics import TextAnalyticsClient
# from azure.core.credentials import AzureKeyCredential
# from dotenv import load_dotenv
# import os

# load_dotenv()

# LANGUAGE_ENDPOINT = os.getenv("LANGUAGE_ENDPOINT")
# LANGUAGE_KEY = os.getenv("LANGUAGE_KEY")

# credential = AzureKeyCredential(LANGUAGE_KEY)
# client = TextAnalyticsClient(endpoint=LANGUAGE_ENDPOINT, credential=credential)

# documents = []

# for filename in ["file.txt"]:
#     with open(os.path.join("financials-data", filename), "r") as f:
#         documents.append("".join(f.readlines()))

# results = client.analyze_sentiment(documents)

# for doc in results:
#     print(f"Overall sentiment: {doc.sentiment}")
#     print(f"Positive sentiment: {doc.confidence_scores.positive}")
#     print(f"Negative sentiment: {doc.confidence_scores.negative}")
#     print(f"Neutral sentiment: {doc.confidence_scores.neutral}")




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
        
        threading.Thread(
            target=run_anomaly_detection_for_event,
            args=(event,),
            daemon=True
        ).start()
        
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise e


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


def get_spark_session():
    """Create and return a properly configured Spark Session with Delta Lake support"""
    try:
        return (SparkSession.builder
                .appName("CDC Delta Processing")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        return None

def create_enhanced_delta_table(event, spark=None):
    """
    Create an enhanced delta table that makes changes easily visible
    and supports Power BI integration
    """
    if spark is None:
        spark = get_spark_session()
        if not spark:
            raise Exception("Failed to create Spark session")
    
    # Extract event details
    company_id = event['company_id']
    table_name = event['table_name']
    event_type = event['event_type']
    timestamp = event['timestamp']
    key_column = event['key_column']
    key_value = event['key_value']
    
    # Define paths
    base_dir = os.environ.get("DELTA_DIR", "./data/delta")
    company_dir = os.path.join(base_dir, company_id)
    table_dir = os.path.join(company_dir, table_name)
    delta_table_path = os.path.join(table_dir, "delta_table")
    
    # Ensure directories exist
    os.makedirs(table_dir, exist_ok=True)
    
    # Create data for the row
    if event_type == 'insert' or event_type == 'update':
        data = event['new_values'] if event['new_values'] else {}
    else:  # delete
        data = event['old_values'] if event['old_values'] else {}
    
    # Skip if no data
    if not data:
        print("No data found in event, skipping")
        return
    
    # Clean data to avoid type inference issues
    clean_data = {}
    for k, v in data.items():
        if pd.isna(v):
            clean_data[k] = None
        elif isinstance(v, (dict, list)):
            clean_data[k] = json.dumps(v)  # Convert complex objects to JSON strings
        else:
            clean_data[k] = v
    
    # Add metadata as simple string types to avoid inference issues
    metadata = {
        '_cdc_event_id': event['event_id'],
        '_cdc_event_type': event_type,
        '_cdc_timestamp': timestamp,
        '_cdc_is_current': "true" if event_type != 'delete' else "false"  # Use string instead of boolean
    }
    
    # Combine data and metadata
    data_with_metadata = {**clean_data, **metadata}
    
    # Create a pandas DataFrame
    pandas_df = pd.DataFrame([data_with_metadata])
    
    # Convert all columns to string type to avoid inference issues
    for col in pandas_df.columns:
        pandas_df[col] = pandas_df[col].astype(str)
    
    # Convert to Spark DataFrame with string schema
    spark_df = spark.createDataFrame(pandas_df)
    
    try:
        # Check if the Delta table exists - MODIFIED THIS PART
        delta_table_exists = DeltaTable.isDeltaTable(spark, delta_table_path)
        
        if delta_table_exists:
            # If table exists, update or insert as appropriate
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            
            if event_type == 'insert' or event_type == 'update':
                # First mark any existing records for this key as not current
                if key_column in data:
                    delta_table.update(
                        condition=f"{key_column} = '{key_value}' AND _cdc_is_current = 'true'",
                        set={
                            "_cdc_is_current": "false"
                        }
                    )
                
                # Then append the new record
                spark_df.write.format("delta").mode("append").save(delta_table_path)
                
            elif event_type == 'delete':
                # For delete, mark existing records as not current
                if key_column in data:
                    delta_table.update(
                        condition=f"{key_column} = '{key_value}' AND _cdc_is_current = 'true'",
                        set={
                            "_cdc_is_current": "false"
                        }
                    )
        else:
            # Delta table doesn't exist yet, create it
            spark_df.write.format("delta").mode("overwrite").save(delta_table_path)
    
        # Create a view for Power BI to more easily access thecurrent  state
        create_current_state_view(spark, delta_table_path, company_id, table_name)
    
    except Exception as e:
        print(f"Error working with Delta table: {e}")
        raise e
    
    return delta_table_path

def create_current_state_view(spark, delta_table_path, company_id, table_name):
    """Create a view that shows only the current state of records for Power BI"""
    try:
        # Read the Delta table
        df = spark.read.format("delta").load(delta_table_path)
        
        # Create a view with only current records
        current_state_df = df.filter(df["_cdc_is_current"] == "true")  # Changed to match string comparison
        
        # Save as a separate Delta table for current state
        current_state_path = delta_table_path + "_current"
        current_state_df.write.format("delta").mode("overwrite").save(current_state_path)
        
        # Create a history view
        history_path = delta_table_path + "_history"
        df.write.format("delta").mode("overwrite").save(history_path)
        
        # Export for Power BI
        export_for_power_bi(spark, company_id, table_name, delta_table_path)
        
    except Exception as e:
        print(f"Error creating view: {e}")

def export_for_power_bi(spark, company_id, table_name, delta_table_path):
    """
    Export the current state of data to a format suitable for Power BI
    """
    try:
        # Define path for Power BI export
        base_dir = os.environ.get("PARQUET_DIR", "./data/parquet")
        company_dir = os.path.join(base_dir, company_id)
        table_dir = os.path.join(company_dir, table_name)
        power_bi_export_path = os.path.join(table_dir, "power_bi_export")
        
        # Ensure directory exists
        os.makedirs(table_dir, exist_ok=True)
        
        # Read the current state Delta table
        current_state_path = delta_table_path + "_current"
        df = spark.read.format("delta").load(current_state_path)
        
        # Export as Parquet for Power BI
        df.write.format("parquet").mode("overwrite").save(power_bi_export_path)
        
        return power_bi_export_path
    
    except Exception as e:
        print(f"Error exporting for Power BI: {e}")
        return None


# Azure Storage settings
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "cdcdata")

class AzureDataLakeManager:
    """Class to manage Azure Data Lake operations for CDC data"""
    
    def __init__(self):
        self.service_client = self._get_service_client()
        self.file_system_client = self._get_file_system_client()
    
    def _get_service_client(self):
        """Get Azure Data Lake service client"""
        if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY:
            logger.error("Azure Storage credentials not found in environment variables")
            return None
        
        try:
            return DataLakeServiceClient(
                account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
                credential=AZURE_STORAGE_ACCOUNT_KEY
            )
        except Exception as e:
            logger.error(f"Error creating Data Lake service client: {e}")
            return None
    
    def _get_file_system_client(self):
        """Get file system client (container)"""
        if not self.service_client:
            return None
        
        try:
            # Try to get existing container
            return self.service_client.get_file_system_client(file_system=AZURE_CONTAINER_NAME)
        except Exception:
            # Create container if it doesn't exist
            try:
                return self.service_client.create_file_system(file_system=AZURE_CONTAINER_NAME)
            except Exception as e:
                logger.error(f"Error creating container: {e}")
                return None
    
    def upload_cdc_event(self, event, df=None):
        """
        Upload CDC event data to Azure Data Lake using an improved structure:
        - /raw/company_id/table_name/year/month/day/event_type_key_timestamp.json (Raw event)
        - /processed/company_id/table_name/current/ (Current state - for Power BI)
        - /processed/company_id/table_name/history/ (Historical changes - for analysis)
        """
        if not self.file_system_client:
            logger.error("File system client not available")
            return False
        
        try:
            # Extract event details
            company_id = event['company_id']
            table_name = event['table_name']
            event_type = event['event_type']
            key_value = event['key_value']
            
            # Create timestamp-based directory structure (for partitioning)
            event_timestamp = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            year = event_timestamp.strftime('%Y')
            month = event_timestamp.strftime('%m')
            day = event_timestamp.strftime('%d')
            
            # Path for raw event data
            raw_dir_path = f"raw/{company_id}/{table_name}/{year}/{month}/{day}"
            timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S%f')
            raw_file_name = f"{event_type}_{key_value}_{timestamp_str}.json"
            
            # Create directory if it doesn't exist
            self._ensure_directory_exists(raw_dir_path)
            
            # Upload raw event data as JSON
            file_client = self.file_system_client.get_file_client(f"{raw_dir_path}/{raw_file_name}")
            file_client.upload_data(json.dumps(event).encode('utf-8'), overwrite=True)
            logger.info(f"Uploaded raw event to: {raw_dir_path}/{raw_file_name}")
            
            # Process and upload data for Power BI integration
            if df is None and (event['new_values'] or event['old_values']):
                # Create DataFrame from event data
                data = event['new_values'] if event['new_values'] else event['old_values']
                if data:
                    # Add metadata
                    data['_cdc_event_id'] = event['event_id']
                    data['_cdc_event_type'] = event_type
                    data['_cdc_timestamp'] = event['timestamp']
                    data['_cdc_is_current'] = True if event_type != 'delete' else False
                    df = pd.DataFrame([data])
            
            if df is not None:
                # Upload to the current state directory (for Power BI)
                self._update_current_state(df, company_id, table_name, key_value, event)
                
                # Append to the history directory (for tracking all changes)
                self._append_to_history(df, company_id, table_name, key_value, event)
            
            return True
            
        except Exception as e:
            logger.error(f"Error uploading CDC event to Azure Data Lake: {e}")
            return False
    
    def _ensure_directory_exists(self, directory_path):
        """Ensure a directory exists in the data lake"""
        try:
            # Try to get the directory client
            self.file_system_client.get_directory_client(directory_path)
        except Exception:
            # Create directory and any parent directories
            parts = directory_path.split('/')
            current_path = ""
            
            for part in parts:
                if part:
                    current_path = f"{current_path}/{part}" if current_path else part
                    try:
                        self.file_system_client.get_directory_client(current_path)
                    except Exception:
                        self.file_system_client.create_directory(current_path)
    
    def _update_current_state(self, df, company_id, table_name, key_value, event):
        """Update the current state data (for Power BI)"""
        # Define the path
        current_state_dir = f"processed/{company_id}/{table_name}/current"
        self._ensure_directory_exists(current_state_dir)
        
        # Convert DataFrame to parquet
        temp_file = f"/tmp/{event['event_id']}.parquet"
        df.to_parquet(temp_file)
        
        # Upload the parquet file
        file_name = f"{key_value}.parquet"
        file_client = self.file_system_client.get_file_client(f"{current_state_dir}/{file_name}")
        
        with open(temp_file, "rb") as file_data:
            # If it's a delete event, delete the file instead of uploading
            if event['event_type'] == 'delete':
                try:
                    file_client.delete_file()
                    logger.info(f"Deleted {file_name} from current state")
                except Exception:
                    # File doesn't exist, nothing to delete
                    pass
            else:
                file_client.upload_data(file_data, overwrite=True)
                logger.info(f"Updated current state: {current_state_dir}/{file_name}")
        
        # Clean up
        os.remove(temp_file)
    
    def _append_to_history(self, df, company_id, table_name, key_value, event):
        """Append to the history data (for analysis and tracking changes)"""
        # Define the path
        history_dir = f"processed/{company_id}/{table_name}/history"
        self._ensure_directory_exists(history_dir)
        
        # Add timestamp to make the filename unique
        timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S%f')
        
        # Convert DataFrame to parquet
        temp_file = f"/tmp/history_{event['event_id']}.parquet"
        df.to_parquet(temp_file)
        
        # Upload the parquet file with a unique name to preserve history
        file_name = f"{key_value}_{event['event_type']}_{timestamp_str}.parquet"
        file_client = self.file_system_client.get_file_client(f"{history_dir}/{file_name}")
        
        with open(temp_file, "rb") as file_data:
            file_client.upload_data(file_data, overwrite=True)
            logger.info(f"Added to history: {history_dir}/{file_name}")
        
        # Clean up
        os.remove(temp_file)
    
    def generate_power_bi_datasets(self):
        """
        Generate consolidated datasets for Power BI to easily import
        - Creates merged parquet files for each table
        - Creates a metadata file with schema information
        """
        if not self.file_system_client:
            logger.error("File system client not available")
            return False
        
        try:
            # Define the path for Power BI datasets
            power_bi_dir = "power_bi"
            self._ensure_directory_exists(power_bi_dir)
            
            # List companies
            companies_dir = "processed"
            companies_paths = []
            
            # Get list of company directories
            companies_client = self.file_system_client.get_directory_client(companies_dir)
            companies_paths = [path.name for path in companies_client.get_paths(recursive=False)]
            
            # Process each company
            for company_path in companies_paths:
                company_id = company_path.split('/')[-1]
                company_dir = f"{companies_dir}/{company_id}"
                
                # Get list of table directories
                company_client = self.file_system_client.get_directory_client(company_dir)
                table_paths = [path.name for path in company_client.get_paths(recursive=False)]
                
                # Process each table
                for table_path in table_paths:
                    table_name = table_path.split('/')[-1]
                    
                    # Create the Power BI dataset directory
                    power_bi_dataset_dir = f"{power_bi_dir}/{company_id}/{table_name}"
                    self._ensure_directory_exists(power_bi_dataset_dir)
                    
                    # Create metadata file with last updated timestamp
                    metadata = {
                        "company_id": company_id,
                        "table_name": table_name,
                        "last_updated": datetime.now().isoformat(),
                        "source": f"processed/{company_id}/{table_name}/current"
                    }
                    
                    metadata_file_client = self.file_system_client.get_file_client(f"{power_bi_dataset_dir}/metadata.json")
                    metadata_file_client.upload_data(json.dumps(metadata).encode('utf-8'), overwrite=True)
                    
                    logger.info(f"Generated Power BI dataset: {power_bi_dataset_dir}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error generating Power BI datasets: {e}")
            return False




Looking for turnkey developer for the following

Khafka to import and capture row / column level CDC changes into PostgresDB from excel spreadsheet imports
Build parquet ,messages of changes against common record over time
Store incremental data into Azure datastore
Connect auto ml to identify anomalies
Connect MS data fabric to present data in Power BI
Enable chatbot to summarize historical data & predict future results

Clarification Questions

To proceed efficiently, we need some clarifications on the following aspects:

Data Ingestion & Kafka Integration

What format will the Excel files be in? (e.g., CSV, XLSX)
CSV
How frequently will data be imported? (Real-time, hourly, daily, etc.)
DAILY INITIALLY
Do you already have a Kafka infrastructure set up, or should we configure it from scratch?
NEED TO SETUP FROM SCRATCH
What specific changes should trigger the CDC process (row updates, column changes, deletes)?
Storage & Data Processing
ALL 3

Do you prefer PostgreSQL as the primary database, or should we consider other options?
LEAST EXPENSIVE INITIALLY FOR POC
Should historical versions of the same record be fully stored or only incremental changes?
INCREMENTAL - HOLD FILES FOR RESTORE
What is the estimated volume of data growth over time? (Daily/Monthly increase in records)
Azure Storage & AutoML
<1 GB TOTAL

Which Azure storage solution do you prefer? (Data Lake, Cosmos DB, SQL Managed Instance, etc.)
DATA LAKE?
Do you already have an Azure environment set up, or should we configure the resources?
NEED TO SETUP FROM SCRATCH
What types of anomalies are you looking to detect? (e.g., fraud detection, data inconsistency, outlier trends)
CHANGE FROM GROUP OUTLIERS

Data Fabric & Power BI Integration

What kind of reports & dashboards do you need in Power BI?
I WILL SETUP. COUNTS, ACTIVITY, DESCRIPTIONS FROM AI
Do you already use Microsoft Data Fabric, or should we set it up?
NEED TO SETUP FROM SCRATCH

Chatbot for Data Analysis & Prediction

What platform should the chatbot be deployed on? (Web app, Teams, Slack, custom app)
SMS & TEAMS
What are the key questions the chatbot should answer? (Historical summaries, trend analysis, etc.)
COUNTS FROM PAST.
Do you need the chatbot to predict future trends or just summarize past data?
AI PREDICTIONS TO LOWER COST / IMPROVE EFFICIENCY



client tasks


1. Infrastructure Setup

2. Data Pipeline & Storage

3. AutoML & Data Fabric Integration

4. Chatbot Development



i  need to implement the 4 task  4. Chatbot Development can you please tell me what i need to  do it in this task please please tell me whole  senerio  of this task
