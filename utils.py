import os
import logging
import datetime
import json
import psycopg2
import redis
from azuredatalake import AzureDataLakeManager
from typing import Dict, Any


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/cdc_data")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Initialize Redis client
redis_client = redis.from_url(REDIS_URL)

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def export_anomaly_alerts_for_power_bi(alert: Dict[str, Any]):
    """Export anomaly alerts in a format usable by Power BI"""
    try:
        azure_manager = AzureDataLakeManager()
        if not azure_manager or not azure_manager.file_system_client:
            logger.error("Azure Data Lake client not available")
            return
        
        # Define the path for Power BI integration
        company_id = alert["company_id"]
        table_name = alert["table_name"]
        power_bi_dir = f"power_bi/{company_id}/{table_name}/anomalies"
        
        # Ensure directory exists
        azure_manager._ensure_directory_exists(power_bi_dir)
        
        # Create the anomalies file
        timestamp = datetime.datetime.now().strftime('%Y%m%d')
        file_client = azure_manager.file_system_client.get_file_client(f"{power_bi_dir}/anomalies_{timestamp}.json")
        file_client.upload_data(json.dumps(alert).encode('utf-8'), overwrite=True)
        
        # Also update the latest file for Power BI to always have current data
        latest_file_client = azure_manager.file_system_client.get_file_client(f"{power_bi_dir}/latest_anomalies.json")
        latest_file_client.upload_data(json.dumps(alert).encode('utf-8'), overwrite=True)
        
        # Create a metadata file for Power BI
        metadata = {
            "company_id": company_id,
            "table_name": table_name,
            "last_updated": datetime.datetime.now().isoformat(),
            "anomaly_count": alert.get("anomalies_detected", 0),
            "dataset_type": "anomaly_detection"
        }
        
        metadata_file_client = azure_manager.file_system_client.get_file_client(f"{power_bi_dir}/metadata.json")
        metadata_file_client.upload_data(json.dumps(metadata).encode('utf-8'), overwrite=True)
        
        logger.info(f"Exported anomaly alerts for Power BI: {power_bi_dir}")
    except Exception as e:
        logger.error(f"Error exporting anomaly alerts for Power BI: {e}")

def check_if_should_run_anomaly_detection(company_id: str, table_name: str) -> bool:
    """Check if we should run anomaly detection for this company/table using Redis"""
    try:
        # Use Redis to track processing counters
        key = f"anomaly_detection:{company_id}:{table_name}"
        
        # Increment counter and get the new value
        counter = redis_client.incr(key)
        
        # Set expiration on the key to automatically clean up old counters (1 day)
        if counter == 1:
            redis_client.expire(key, 86400)  # 24 hours in seconds
            
        # Run anomaly detection every 10 increments
        if counter >= 10:
            # Reset counter
            redis_client.set(key, 0)
            return True
            
        return False
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        return True 
    except Exception as e:
        logger.error(f"Error checking if should run anomaly detection: {e}")
        return True
    
def generate_anomaly_alerts(company_id: str, table_name: str, result: Dict[str, Any]):
    """Generate alerts for detected anomalies"""
    try:
        # Create an alert object
        alert = {
            "company_id": company_id,
            "table_name": table_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "anomalies_detected": result.get("anomalies_detected", 0),
            "anomaly_points": [r for r in result.get("results", []) if r.get("is_anomaly", False)]
        }
        
        # Store in database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO anomaly_alerts
                    (company_id, table_name, alert_time, alert_data)
                    VALUES (%s, %s, %s, %s)
                """, (
                    company_id,
                    table_name,
                    datetime.datetime.now(),
                    json.dumps(alert)
                ))
                conn.commit()
        
        # Could also send email/Slack notification here
        
        # Export for Power BI
        export_anomaly_alerts_for_power_bi(alert)
        
    except Exception as e:
        logger.error(f"Error generating anomaly alerts: {e}")