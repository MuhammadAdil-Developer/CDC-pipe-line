import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any
from dotenv import load_dotenv
from azure.ai.anomalydetector import AnomalyDetectorClient
from azure.ai.anomalydetector.models import TimeSeriesPoint
from azure.core.credentials import AzureKeyCredential
from azure.storage.filedatalake import DataLakeServiceClient
from utils import generate_anomaly_alerts, check_if_should_run_anomaly_detection

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Azure Anomaly Detector settings
ANOMALY_DETECTOR_KEY = os.environ.get("ANOMALY_DETECTOR_KEY", "")
ANOMALY_DETECTOR_ENDPOINT = os.environ.get("ANOMALY_DETECTOR_ENDPOINT", "")

# Azure Data Lake settings
AZURE_STORAGE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "")
AZURE_STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "cdcdata")

class AnomalyDetectionManager:
    """Class to manage anomaly detection operations using Azure AutoML"""

    def __init__(self):
        print("Initializing AnomalyDetectionManager...")  # Print statement
        logger.info("Initializing AnomalyDetectionManager...")  # Logging statement
        self.anomaly_client = self._get_anomaly_client()
        self.datalake_client = self._get_datalake_client()

    def _get_anomaly_client(self):
        """Initialize the Azure Anomaly Detector client"""
        print("Initializing Azure Anomaly Detector client...")
        logger.info("Initializing Azure Anomaly Detector client...")
        
        if not ANOMALY_DETECTOR_KEY or not ANOMALY_DETECTOR_ENDPOINT:
            error_msg = "Azure Anomaly Detector credentials not found in environment variables"
            print(error_msg)
            logger.error(error_msg)
            return None

        try:
            # Import the DetectRequest class here
            from azure.ai.anomalydetector.models import DetectRequest
            
            print("Creating Anomaly Detector client...")
            logger.info("Creating Anomaly Detector client...")
            return AnomalyDetectorClient(
                AzureKeyCredential(ANOMALY_DETECTOR_KEY),
                ANOMALY_DETECTOR_ENDPOINT
            )
        except Exception as e:
            error_msg = f"Error creating Anomaly Detector client: {e}"
            print(error_msg)
            logger.error(error_msg)
            return None

    def _get_datalake_client(self):
        """Get Azure Data Lake service client"""
        print("Initializing Azure Data Lake client...")  # Print statement
        logger.info("Initializing Azure Data Lake client...")  # Logging statement
        if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY:
            error_msg = "Azure Storage credentials not found in environment variables"
            print(error_msg)  # Print statement
            logger.error(error_msg)  # Logging statement
            return None

        try:
            print("Creating Azure Data Lake client...")  # Print statement
            logger.info("Creating Azure Data Lake client...")  # Logging statement
            service_client = DataLakeServiceClient(
                account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
                credential=AZURE_STORAGE_ACCOUNT_KEY
            )
            return service_client.get_file_system_client(file_system=AZURE_CONTAINER_NAME)
        except Exception as e:
            error_msg = f"Error creating Data Lake client: {e}"
            print(error_msg)  # Print statement
            logger.error(error_msg)  # Logging statement
            return None

    def detect_anomalies_in_company_data(self, company_id, table_name, metric_column, time_window_days=30):
        """
        Detect anomalies in the specified company data
        """
        print(f"Starting anomaly detection for company_id={company_id}, table_name={table_name}, metric_column={metric_column}")  # Print statement
        logger.info(f"Starting anomaly detection for company_id={company_id}, table_name={table_name}, metric_column={metric_column}")  # Logging statement

        if not self.anomaly_client or not self.datalake_client:
            error_msg = "Clients not properly initialized"
            print(error_msg)  # Print statement
            logger.error(error_msg)  # Logging statement
            return {"success": False, "error": "Clients not initialized"}

        try:
            from azure.ai.anomalydetector.models import DetectRequest
            logger.info("Fetching time series data from Azure Data Lake...")  # Logging statement
            time_series_data = self._get_time_series_data(company_id, table_name, metric_column, time_window_days)

            if not time_series_data or len(time_series_data) < 12:
                error_msg = "Insufficient data points for anomaly detection"
                print(error_msg)  # Print statement
                logger.warning(error_msg)  # Logging statement
                return {"success": False, "error": error_msg}

            print(f"Preparing time series data for anomaly detection (data points: {len(time_series_data)})...")  # Print statement
            logger.info(f"Preparing time series data for anomaly detection (data points: {len(time_series_data)})...")  # Logging statement
            time_series_points = [
                TimeSeriesPoint(timestamp=point["timestamp"], value=point["value"])
                for point in time_series_data
            ]

            print("Setting up anomaly detection request...")  # Print statement
            logger.info("Setting up anomaly detection request...")  # Logging statement
            request = DetectRequest(
                series=time_series_points,
                granularity="daily",
                max_anomaly_ratio=0.25,
                sensitivity=95
            )

            print("Calling Azure Anomaly Detector API...")  # Print statement
            logger.info("Calling Azure Anomaly Detector API...")  # Logging statement
            response = self.anomaly_client.detect_entire_series(request)

            print("Processing anomaly detection results...")  # Print statement
            logger.info("Processing anomaly detection results...")  # Logging statement
            results = self._process_anomaly_results(time_series_data, response)
            self._store_anomaly_results(company_id, table_name, metric_column, results)

            anomalies_count = len([r for r in results if r["is_anomaly"]])
            total_points = len(results)
            print(f"Anomaly detection complete: {anomalies_count}/{total_points} anomalies detected")  # Print statement
            logger.info(f"Anomaly detection complete: {anomalies_count}/{total_points} anomalies detected")  # Logging statement

            return {
                "success": True,
                "company_id": company_id,
                "table_name": table_name,
                "metric": metric_column,
                "anomalies_detected": anomalies_count,
                "total_points": total_points,
                "results": results
            }

        except Exception as e:
            error_msg = f"Error detecting anomalies: {e}"
            print(error_msg)  # Print statement
            logger.error(error_msg)  # Logging statement
            return {"success": False, "error": str(e)}
    
    def _get_time_series_data(self, company_id, table_name, metric_column, time_window_days):
        """
        Extract time series data from the history directory in Azure Data Lake
        
        Returns:
            List of dictionaries with timestamp and value
        """
        # Path to history directory
        history_dir = f"processed/{company_id}/{table_name}/history"
        
        try:
            # List all parquet files in the history directory
            directory_client = self.datalake_client.get_directory_client(history_dir)
            paths = list(directory_client.get_paths())
            
            if not paths:
                logger.warning(f"No history data found for {company_id}/{table_name}")
                return []
            
            # Calculate start date for time window
            end_date = datetime.now()
            start_date = end_date - timedelta(days=time_window_days)
            
            # Aggregate daily counts of changes
            daily_counts = {}
            
            for path_item in paths:
                path = path_item.name
                
                # Extract timestamp from the filename (assuming format: key_type_timestamp.parquet)
                parts = os.path.basename(path).split('_')
                if len(parts) >= 3:
                    # Try to parse the timestamp
                    try:
                        file_date = datetime.strptime(parts[-1].split('.')[0][:8], '%Y%m%d')
                        
                        # Skip if outside our time window
                        if file_date < start_date or file_date > end_date:
                            continue
                            
                        date_str = file_date.strftime('%Y-%m-%d')
                        
                        # For simplicity, we're just counting changes per day
                        # Later we could actually read the parquet files to analyze the metric_column values
                        if date_str in daily_counts:
                            daily_counts[date_str] += 1
                        else:
                            daily_counts[date_str] = 1
                    except Exception as e:
                        logger.warning(f"Couldn't parse date from {path}: {e}")
            
            # Ensure we have data for each day in the time window (fill gaps with zeros)
            current_date = start_date
            time_series = []
            
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                time_series.append({
                    "timestamp": date_str,
                    "value": float(daily_counts.get(date_str, 0))
                })
                current_date += timedelta(days=1)
            
            return time_series
            
        except Exception as e:
            logger.error(f"Error getting time series data: {e}")
            return []
    
    def _process_anomaly_results(self, time_series_data, response):
        """Process anomaly detection results"""
        results = []
        
        for i, point in enumerate(time_series_data):
            results.append({
                "timestamp": point["timestamp"],
                "value": point["value"],
                "is_anomaly": response.is_anomaly[i],
                "score": response.anomaly_score[i],
                "expected_value": response.expected_values[i] if response.expected_values else None
            })
        
        return results
    
    def _store_anomaly_results(self, company_id, table_name, metric_column, results):
        """Store anomaly detection results in Azure Data Lake"""
        try:
            # Define the path for anomaly results
            anomaly_dir = f"analytics/{company_id}/{table_name}/anomalies"
            
            # Ensure the directory exists
            self._ensure_directory_exists(anomaly_dir)
            
            # Create a timestamp for the file
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            file_name = f"anomaly_{metric_column}_{timestamp}.json"
            
            # Upload the results as JSON
            file_client = self.datalake_client.get_file_client(f"{anomaly_dir}/{file_name}")
            file_client.upload_data(json.dumps(results).encode('utf-8'), overwrite=True)
            
            logger.info(f"Stored anomaly detection results at {anomaly_dir}/{file_name}")
            
            # For Power BI integration, also store the latest results in a known location
            latest_file_client = self.datalake_client.get_file_client(f"{anomaly_dir}/latest_{metric_column}.json")
            latest_file_client.upload_data(json.dumps(results).encode('utf-8'), overwrite=True)
            
        except Exception as e:
            logger.error(f"Error storing anomaly results: {e}")
    
    def _ensure_directory_exists(self, directory_path):
        """Ensure a directory exists in the data lake"""
        try:
            # Try to get the directory client
            self.datalake_client.get_directory_client(directory_path)
        except Exception:
            # Create directory and any parent directories
            parts = directory_path.split('/')
            current_path = ""
            
            for part in parts:
                if part:
                    current_path = f"{current_path}/{part}" if current_path else part
                    try:
                        self.datalake_client.get_directory_client(current_path)
                    except Exception:
                        self.datalake_client.create_directory(current_path)

# Helper function to run anomaly detection for all tables of a company
def detect_anomalies_for_company(company_id, tables=None):
    """
    Run anomaly detection for all tables of a company
    
    Args:
        company_id: The ID of the company
        tables: List of tables to analyze, defaults to all supported tables
    """
    if tables is None:
        tables = ["Asset", "Location", "WorkOrder", "PreventiveMaintenance", "PurchaseOrder"]
    
    detector = AnomalyDetectionManager()
    results = {}
    
    for table in tables:
        # For each table, detect anomalies in the number of changes (CDC events)
        table_result = detector.detect_anomalies_in_company_data(
            company_id=company_id,
            table_name=table,
            metric_column="changes_count",  # We're analyzing the count of changes
            time_window_days=30  # Look at the last 30 days
        )
        results[table] = table_result
    
    return results

def run_anomaly_detection_for_event(event: Dict[str, Any]):
    """Run anomaly detection for a specific event"""
    try:
        company_id = event['company_id']
        table_name = event['table_name']
        
        # Print environment variables to debug
        print(f"ANOMALY_DETECTOR_KEY is {'set' if ANOMALY_DETECTOR_KEY else 'not set'}")
        print(f"ANOMALY_DETECTOR_ENDPOINT is {'set' if ANOMALY_DETECTOR_ENDPOINT else 'not set'}")
        print(f"AZURE_STORAGE_ACCOUNT_NAME is {'set' if AZURE_STORAGE_ACCOUNT_NAME else 'not set'}")
        print(f"AZURE_STORAGE_ACCOUNT_KEY is {'set' if AZURE_STORAGE_ACCOUNT_KEY else 'not set'}")
        
        # Debugging: Print event details
        print(f"Received event for anomaly detection: company_id={company_id}, table_name={table_name}")
        logger.info(f"Received event for anomaly detection: company_id={company_id}, table_name={table_name}")
        
        # Check if we should run anomaly detection
        should_run = check_if_should_run_anomaly_detection(company_id, table_name)
        
        # Debugging: Print whether anomaly detection should run
        print(f"Should run anomaly detection: {should_run}")
        logger.info(f"Should run anomaly detection: {should_run}")
        
        if should_run:
            logger.info(f"Triggering anomaly detection for {company_id}/{table_name}")
            detector = AnomalyDetectionManager()
            
            # Verify clients are properly initialized
            if not detector.anomaly_client:
                print("Anomaly Detector client is not initialized")
                logger.error("Anomaly Detector client is not initialized")
                return
                
            if not detector.datalake_client:
                print("Data Lake client is not initialized")
                logger.error("Data Lake client is not initialized")
                return
            
            # Run anomaly detection
            result = detector.detect_anomalies_in_company_data(
                company_id=company_id,
                table_name=table_name,
                metric_column="changes_count",
                time_window_days=30
            )
            
            # Log the results
            if result["success"]:
                anomalies_count = result.get("anomalies_detected", 0)
                total_points = result.get("total_points", 0)
                logger.info(f"Anomaly detection complete: {anomalies_count}/{total_points} anomalies detected")
                
                # If anomalies detected, generate alerts
                if anomalies_count > 0:
                    generate_anomaly_alerts(company_id, table_name, result)
    
    except Exception as e:
        logger.error(f"Error in anomaly detection: {e}")
        # Print the full stack trace for better debugging
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        company_id = sys.argv[1]
        tables = sys.argv[2].split(',') if len(sys.argv) > 2 else None
        
        print(f"Running anomaly detection for company: {company_id}")
        results = detect_anomalies_for_company(company_id, tables)
        
        for table, result in results.items():
            if result["success"]:
                print(f"{table}: {result['anomalies_detected']} anomalies detected out of {result['total_points']} data points")
            else:
                print(f"{table}: Error - {result.get('error', 'Unknown error')}")
    else:
        print("Usage: python automl_anomaly_detection.py <company_id> [table1,table2,...]")