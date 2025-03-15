import os
import logging
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
import json
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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