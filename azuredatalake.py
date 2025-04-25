import os
import logging
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
import json
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import uuid

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
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "newcdcdata")

class AzureDataLakeManager:
    """Class to manage Azure Data Lake operations for CDC data"""
    
    def __init__(self):
        self.service_client = self._get_service_client()
        self.file_system_client = self._get_file_system_client()
    
    def _sanitize_name(self, name):
        """Convert 'Balance Sheet' → 'Balance_Sheet' for filesystem safety"""
        return "".join(
            c if c.isalnum() else "_" 
            for c in str(name).strip()
        ).strip("_")

    
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
    
    def _get_safe_temp_dir(self):
        """Returns a safe temp directory path that definitely works"""
        # Try several possible locations
        possible_dirs = [
            os.path.join(os.getcwd(), "temp_uploads"),  # 1. Local temp
            "/tmp/cdc_uploads",                         # 2. System temp
            os.path.expanduser("~/cdc_temp_uploads")    # 3. Home directory
        ]
        
        for temp_dir in possible_dirs:
            try:
                os.makedirs(temp_dir, exist_ok=True, mode=0o777)
                # Test writing a file
                test_file = os.path.join(temp_dir, "test.tmp")
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                return temp_dir
            except Exception:
                continue
        
        raise Exception("Could not find a writable temp directory")

    def _get_safe_filename(self, name):
        """Returns a filesystem-safe version of the name"""
        return "".join(c for c in name if c.isalnum() or c in ('_', '-')).rstrip()
    
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

    def upload_consolidated_data(self, company_id: str, df: pd.DataFrame):
        """Upload consolidated data as a single Parquet file with simple structure"""
        try:
            # Sanitize company ID
            company_id = self._sanitize_name(company_id)
            
            # Create company directory if it doesn't exist
            self._ensure_directory_exists(company_id)
            
            # Upload the consolidated Parquet file
            file_path = f"{company_id}/consolidated_data.parquet"
            self.upload_parquet(df, file_path)
            
            logger.info(f"✅ Consolidated data uploaded to: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error uploading consolidated data: {str(e)}")
            return False
    
    def process_and_upload_data(self, event, df):
        """Process and upload data to Azure Data Lake in the required structure"""
        try:
            company_id = self._sanitize_name(event['company_id'])
            table_name = self._sanitize_name(event['table_name'])
            
            # 1. Create financial metric files (original logic)
            self._create_financial_files(df, company_id, table_name)
            
            # 2. Create operational data file (original logic)
            self._create_operational_data(df, company_id, table_name)
            
            # 3. NEW: Upload combined CSV to Delta Lake (for client's requirement)
            self._upload_combined_csv_to_delta(df, company_id, table_name)
            
            return True
        except Exception as e:
            logger.error(f"Error processing and uploading data: {str(e)}")
            return False

    def _upload_combined_csv_to_delta(self, df, company_id, table_name):
        """Upload combined CSV in Delta format (Client's requirement)"""
        try:
            # 1. Prepare Delta Lake path
            delta_path = f"{company_id}/combined_financials"
            self._ensure_directory_exists(delta_path)
            
            # 2. Convert DataFrame to Delta format
            delta_df = df.copy()
            
            # 3. Add metadata columns (if missing)
            if "SECTOR" not in delta_df.columns:
                delta_df.insert(0, "SECTOR", "GAS UTILITY")
            if "COMPANY" not in delta_df.columns:
                delta_df.insert(1, "COMPANY", company_id)
            
            # 4. Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y-%m-%d %H_%M")
            filename = f"FINANCIAL_DATA_{timestamp}.parquet"
            filepath = f"{delta_path}/{filename}"
            
            # 5. Upload to Delta Lake
            self.upload_parquet(delta_df, filepath)
            
            logger.info(f"✅ Combined CSV uploaded to Delta Lake: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload combined CSV to Delta: {str(e)}")
            return False

    def _create_financial_files(self, df, company_id, table_name):
        """Create financial metric files"""
        try:
            # Clean the dataframe
            df = df.dropna(how='all')
            df = df.loc[:, ~df.columns.astype(str).str.contains('Unnamed|\.', regex=True)]
            
            # Get metrics from first column and dates from header
            metrics = df.iloc[:, 0].tolist()
            dates = df.columns[1:].tolist()
            processing_date = datetime.now().strftime('%Y%m%d')
            base_path = f"{company_id}/{table_name}"
            self._ensure_directory_exists(base_path)
            
            created_files = 0
            
            for i, metric in enumerate(metrics):
                if not isinstance(metric, str) or metric.strip() == '':
                    continue
                    
                safe_name = (
                    metric.strip()
                    .replace(" ", "_")
                    .replace("&", "and")
                    .replace("/", "_")
                    .replace("%", "pct")
                    .replace("(", "")
                    .replace(")", "")
                )
                filename = f"{safe_name}_{processing_date}.parquet"
                filepath = f"{base_path}/{filename}"
                
                values = df.iloc[i, 1:].values
                metric_df = pd.DataFrame({
                    'Date': dates,
                    safe_name: values
                })
                
                try:
                    metric_df[safe_name] = pd.to_numeric(metric_df[safe_name], errors='coerce')
                except:
                    pass
                    
                if not metric_df.empty:
                    self.upload_parquet(metric_df, filepath)
                    created_files += 1
                    
            logger.info(f"Created {created_files} financial metric files for {company_id}/{table_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating financial files: {str(e)}")
            return False

    def _create_operational_data(self, df, company_id, table_name):
        """Create operational data file in required format"""
        try:
            # Prepare operational data path
            operational_dir = f"{company_id}"
            self._ensure_directory_exists(operational_dir)
            
            # Create filename with current date
            processing_date = datetime.now().strftime('%Y%m%d')
            filename = f"{table_name.lower()}_operational_{processing_date}.parquet"
            filepath = f"{operational_dir}/{filename}"
            
            # Create operational DataFrame with key columns
            operational_cols = []
            
            # Add date column if exists
            if 'Date' in df.columns:
                operational_cols.append('Date')
                
            # Add any ID columns (case insensitive check)
            id_cols = [col for col in df.columns if any(x in col.lower() for x in ['id', 'code', 'key'])]
            operational_cols.extend(id_cols)
            
            # If no specific ID columns, use first 3 columns as operational data
            if not operational_cols and len(df.columns) >= 3:
                operational_cols = df.columns[:3].tolist()
                
            # Create and upload the operational file
            if operational_cols:
                op_df = df[operational_cols].copy()
                op_df = op_df.dropna(how='all')
                op_df['company_id'] = company_id
                
                self.upload_parquet(op_df, filepath)
                logger.info(f"Created operational data file: {filepath}")
                return True
                
            logger.warning(f"No operational columns identified for {company_id}/{table_name}")
            return False
        except Exception as e:
            logger.error(f"Error creating operational data: {str(e)}")
            return False

    def _get_current_version(self, company_id, table_name):
        """Get current version number from _delta_log"""
        try:
            log_dir = f"{company_id}/{table_name}/_delta_log"
            files = self.list_files(log_dir)
            versions = [int(f.split('.')[0]) for f in files if f.endswith('.json')]
            return max(versions) if versions else 0
        except:
            return 0

    def _get_next_version(self, company_id, table_name):
        """Get next version number"""
        return self._get_current_version(company_id, table_name) + 1

    def upload_parquet(self, df, path):
        """Upload DataFrame as parquet file"""
        temp_file = f"/tmp/{uuid.uuid4()}.parquet"
        df.to_parquet(temp_file)
        self.upload_file(temp_file, path)
        os.remove(temp_file)
    
    def upload_json(self, data, path):
        """Upload JSON data"""
        temp_file = f"/tmp/{uuid.uuid4()}.json"
        with open(temp_file, 'w') as f:
            json.dump(data, f)
        self.upload_file(temp_file, path)
        os.remove(temp_file)


    def upload_file(self, local_path, remote_path):
        """Generic file upload"""
        try:
            file_client = self.file_system_client.get_file_client(remote_path)
            with open(local_path, "rb") as file_data:
                file_client.upload_data(file_data, overwrite=True)
            return True
        except Exception as e:
            logger.error(f"File upload failed: {str(e)}")
            return False
    
    def list_files(self, directory_path):
        """List files in directory"""
        try:
            directory_client = self.file_system_client.get_directory_client(directory_path)
            return [f.name.split('/')[-1] for f in directory_client.get_paths()]
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []

    def upload_cdc_event(self, event, company_id, table_name, df=None):
        """Upload data with dynamic metric-based structure"""
        try:
            # 1. Prepare DataFrame if not provided
            if df is None:
                data = event.get("new_values") or event.get("old_values") or {}
                df = pd.DataFrame([{
                    **data,
                    "_company_id": company_id,
                    "_table_name": table_name,
                    "_event_type": event["event_type"],
                    "_timestamp": event["timestamp"]
                }])

            # 2. Sanitize paths
            company_dir = self._sanitize_name(company_id)  # "AEP" → "AEP"
            table_dir = self._sanitize_name(table_name)    # "Balance Sheet" → "Balance_Sheet"
            base_path = f"{company_dir}/{table_dir}"
            self._ensure_directory_exists(base_path)

            # 3. Extract metrics (exclude metadata columns starting with '_')
            metrics = [col for col in df.columns if not col.startswith("_")]

            # 4. Upload each metric as separate file
            for metric in metrics:
                metric_file = f"{metric}_{datetime.now().strftime('%Y%m%d')}.parquet"
                metric_path = f"{base_path}/{metric_file}"
                
                # Upload only this metric's data
                self.upload_parquet(df[[metric]], metric_path)
                logger.info(f"Uploaded metric: {metric_path}")

            # 5. Add metadata for Power BI
            metadata = {
                "company": company_id,
                "table": table_name,
                "metrics": metrics,
                "last_updated": datetime.now().isoformat()
            }
            self.upload_json(metadata, f"{base_path}/_metadata.json")

            return True

        except Exception as e:
            logger.error(f"Dynamic metric upload failed: {str(e)}")
            return False

    
    def get_latest_anomalies(self, limit: int = 10) -> list:
        """Get latest anomaly reports from data lake - for chatbot"""
        try:
            directory_client = self.file_system_client.get_directory_client("analytics/anomalies")
            anomaly_files = list(directory_client.get_paths())
            
            # Get most recent file
            latest_file = sorted(anomaly_files, key=lambda x: x.last_modified, reverse=True)[0]
            file_client = directory_client.get_file_client(latest_file.name)
            
            download = file_client.download_file()
            return json.loads(download.readall())
        except Exception as e:
            logging.error(f"Error fetching anomalies: {str(e)}")
            return []

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
        """Updated for new structure"""
        try:
            # Get all company folders
            companies = []
            for item in self.file_system_client.get_paths():
                if item.is_directory and '/' not in item.name:
                    companies.append(item.name)
            
            # Create combined metadata
            metadata = {
                "companies": companies,
                "last_updated": datetime.now().isoformat(),
                "note": "Access files directly from company folders"
            }
            
            # Upload metadata
            metadata_client = self.file_system_client.get_file_client("_metadata.json")
            metadata_client.upload_data(json.dumps(metadata).encode('utf-8'))
            
            return True
        except Exception as e:
            logger.error(f"Metadata generation failed: {e}")
            return False
