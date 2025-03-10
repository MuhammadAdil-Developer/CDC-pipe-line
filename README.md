# Kafka CDC Pipeline with PostgreSQL Integration

describes the Change Data Capture (CDC) pipeline implemented for processing financial data CSV files. The system tracks changes in data over time, maintains historical records, and provides efficient storage and retrieval mechanisms.

## Architecture

The CDC pipeline consists of two main components:

1. **Data Ingestion API**: A FastAPI application that receives CSV files, detects changes, and publishes events to Kafka.
2. **Data Consumer**: A Kafka consumer that processes change events, updates the database, and generates Parquet files.

### Technologies Used

- **FastAPI**: Web framework for the API
- **Apache Kafka**: Event streaming platform
- **PostgreSQL**: Relational database for structured data storage
- **Apache Parquet**: Columnar storage format for efficient analytics
- **Python**: Programming language for implementation

## Data Flow

```
┌────────┐    ┌───────────┐    ┌─────────┐    ┌─────────────┐    ┌──────────┐
│  CSV   │ -> │ Ingestion │ -> │  Kafka  │ -> │   Consumer  │ -> │ Database │
│  File  │    │    API    │    │ Broker  │    │  Processing │    │ & Parquet│
└────────┘    └───────────┘    └─────────┘    └─────────────┘    └──────────┘
```

## API Endpoints

### File Upload

**Endpoint**: `POST /upload`

**Response**:
```json
{
  "message": "File processed successfully",
  "file_id": "so-financials_a1b2c3d4",
  "company_id": "so-financials",
  "changes_detected": 3
}
```

### Health Check

**Endpoint**: `GET /health`

**Description**: Checks system health and connectivity

**Response**:
```json
{
  "status": "UP",
  "kafka": "UP",
  "timestamp": "2025-03-10T12:34:56.789Z"
}
```

## Database Schema

The system maintains primary tables:

### 1. CDC Events

Stores all change events detected by the system.

### 2. Company Data

Stores the current and historical company data with temporal validity.


## Data Storage

### File Storage

The system maintains the following directories:

- **Current**: Latest version of each company's data
- **Archive**: Historical versions of data files with timestamps
- **Parquet**: Structured storage for analytical queries

### Parquet File Structure

Parquet files are organized in the following directory structure:
```
/data/parquet/
  └── company_id/
      └── table_name/
          ├── insert_YYYYMMDDHHMMSS_key_value.parquet
          ├── update_YYYYMMDDHHMMSS_key_value.parquet
          └── delete_YYYYMMDDHHMMSS_key_value.parquet
```

## Configuration Options

The system can be configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| DATA_DIR | Base directory for file storage | ./data |
| KAFKA_BOOTSTRAP_SERVERS | Kafka servers | localhost:9092 |
| KAFKA_TOPIC | Kafka topic for CDC events | cdc-events |
| DATABASE_URL | PostgreSQL connection string | postgresql://postgres:postgres@localhost:5432/cdc_data |
| PARQUET_DIR | Directory for Parquet files | ./data/parquet |
| CONSUMER_GROUP | Kafka consumer group ID | cdc-processor |


## Data Retention and Management

- CDC Events are stored indefinitely for audit purposes
- Company data maintains full history with temporal validity
- Parquet files provide efficient storage for analytical workloads
