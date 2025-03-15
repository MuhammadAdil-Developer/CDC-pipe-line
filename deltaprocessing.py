import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, lit, current_timestamp
from delta.tables import DeltaTable

# Initialize Spark Session

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