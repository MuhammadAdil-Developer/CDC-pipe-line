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
    Creates delta tables with:
    - Flat company folder structure (DUK/, AEP/)
    - Company IDs in file names
    - Power BI friendly structure
    """
    if spark is None:
        spark = get_spark_session()
        if not spark:
            raise Exception("Spark session unavailable")

    # Extract and standardize company ID (DUK, AEP, etc.)
    company_id = event['company_id'].strip().upper()
    table_name = event['table_name']
    event_type = event['event_type']
    key_value = str(event['key_value'])
    timestamp = event['timestamp']

    # New flat directory structure
    base_dir = os.environ.get("DELTA_DIR", "./data/delta")
    company_dir = os.path.join(base_dir, company_id)
    os.makedirs(company_dir, exist_ok=True)

    # Delta table path with company prefix
    delta_table_path = os.path.join(company_dir, f"{table_name}_delta")

    # Create data with metadata columns
    data = {}
    if event_type in ['insert', 'update'] and event['new_values']:
        data = event['new_values']
    elif event_type == 'delete' and event['old_values']:
        data = event['old_values']

    if not data:
        logger.warning(f"No data found in {event_type} event")
        return None

    # Clean and enhance data
    enhanced_data = {
        **{k: (None if pd.isna(v) else v) for k,v in data.items()},
        '_event_id': event['event_id'],
        '_company_id': company_id,
        '_event_type': event_type,
        '_timestamp': timestamp,
        '_is_current': event_type != 'delete'
    }

    # Create DataFrame
    pdf = pd.DataFrame([enhanced_data])
    spark_df = spark.createDataFrame(pdf)

    try:
        # Check if Delta table exists
        if DeltaTable.isDeltaTable(spark, delta_table_path):
            delta_table = DeltaTable.forPath(spark, delta_table_path)
            
            # For updates/inserts, mark previous version as not current
            if event_type in ['insert', 'update']:
                delta_table.update(
                    condition=f"{event['key_column']} = '{key_value}' AND _is_current = true",
                    set={"_is_current": "false"}
                )
            
            # For deletes, just mark as not current
            elif event_type == 'delete':
                delta_table.update(
                    condition=f"{event['key_column']} = '{key_value}' AND _is_current = true",
                    set={"_is_current": "false"}
                )
                return delta_table_path

            # Append new version
            spark_df.write.format("delta").mode("append").save(delta_table_path)
        else:
            # Create new Delta table
            spark_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(delta_table_path)

        # Create Power BI view
        create_powerbi_view(spark, delta_table_path, company_id, table_name)
        
        return delta_table_path

    except Exception as e:
        logger.error(f"Delta processing failed: {str(e)}")
        raise e

def create_powerbi_view(spark, delta_path, company_id, table_name):
    """Creates Power BI optimized view"""
    try:
        # Read delta table
        df = spark.read.format("delta").load(delta_path)
        
        # Create current records view
        current_df = df.filter("_is_current = true")
        
        # Save in Power BI friendly location
        powerbi_dir = os.path.join(os.path.dirname(delta_path), "powerbi")
        current_df.write.format("parquet") \
            .mode("overwrite") \
            .save(os.path.join(powerbi_dir, f"{table_name}_current_{company_id}"))
            
    except Exception as e:
        logger.error(f"Power BI view creation failed: {str(e)}")

        
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