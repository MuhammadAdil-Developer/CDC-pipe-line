# CDC Data Pipeline Usage Guide

## Purpose
The **CDC Data Pipeline** is a FastAPI-based application that processes financial and operational data, uploads it to **Azure Data Lake Storage** in Delta Lake or Parquet format, and enables visualization in **Power BI**. This guide explains how to:
- Upload data files (CSV, XLSX, DOCX, ZIP) to the pipeline.
- Store processed data in Azure Data Lake.
- Connect Azure Data Lake to Power BI for reporting.

---

## Prerequisites
- **Azure Resources**:
  - Azure Data Lake Storage Gen2 account with a container (e.g., `newcdcdata`).
  - Azure credentials (connection string or account name/key).
- **Software**:
  - Access to the pipeline’s API (running at `20.9.138.28:8000`).
  - Power BI Desktop or Power BI Service.
  - Postman (optional, for testing API endpoints).
- **Data Files**:
  - CSV, XLSX, DOCX, or ZIP files with structured data (e.g., financial spreadsheets with a date column).

---

## How to Use the Pipeline

### Step 1: Upload Files
The pipeline accepts files via two API endpoints. Use tools like `curl` or Postman to upload files.

#### Upload a Single File (CSV, XLSX, DOCX)
- **Endpoint**: `POST /upload`
- **Description**: Upload a single file for change data capture (CDC) processing.
- **Supported Types**: Asset, Location, WorkOrder, PreventiveMaintenance, PurchaseOrder
- **Command**:
  ```bash
  curl -X POST "http://20.9.138.28:8000/upload" \
       -F "company_id=AEP" \
       -F "table_type=Asset" \
       -F "file=@/path/to/data.csv"
  ```
  - Replace `/path/to/data.csv` with your file path (e.g., `financials.csv`).
  - `company_id`: Your company identifier (e.g., `AEP`).
  - `table_type`: One of the supported types (e.g., `Asset`).
- **Example File Format** (CSV):
  ```
  Date,Value
  2023-01-01,100
  2023-01-02,150
  ```
- **Result**:
  - The pipeline detects changes (inserts, updates, deletes) based on the first column (e.g., `Date`).
  - Processed data is uploaded to Azure Data Lake under `<company_id>/<table_name>` (e.g., `AEP/Asset`).
  - Response:
    ```json
    {
      "message": "File processed successfully",
      "file_id": "<unique-id>",
      "company_id": "AEP",
      "table_name": "Asset",
      "changes_detected": 2
    }
    ```

#### Upload a ZIP File
- **Endpoint**: `POST /upload-zip`
- **Description**: Upload a ZIP file containing multiple XLSX spreadsheets, consolidated into a single Parquet file.
- **Command**:
  ```bash
  curl -X POST "http://20.9.138.28:8000/upload-zip" \
       -F "file=@/path/to/financials.zip" \
       -F "output_folder=."
  ```
  - Replace `/path/to/financials.zip` with your ZIP file path.
- **Example ZIP Content**:
  - `AEP-BalanceSheet.xlsx` with sheets like `Annual`, `Quarterly`.
  - Each sheet should have a table with a date column (e.g., `Date,Revenue,Profit`).
- **Result**:
  - The pipeline extracts XLSX files, consolidates data, and uploads a single Parquet file to Azure Data Lake under `<company_id>/consolidated_data.parquet` (e.g., `AEP/consolidated_data.parquet`).
  - Response:
    ```json
    {
      "message": "ZIP file processed successfully. Data consolidated into single Parquet file.",
      "file_id": "<unique-id>",
      "company_id": "AEP",
      "table_name": "consolidated_data",
      "changes_detected": 100
    }
    ```

### Step 2: Data Storage in Azure Delta Lake
- **How It Works**:
  - **Single File Upload** (`/upload`):
    - Financial metrics are stored as individual Parquet files (e.g., `Revenue_20250426.parquet`) under `<company_id>/<table_name>`.
    - Operational data (e.g., IDs, dates) is stored as `<table_name>_operational_20250426.parquet`.
    - A combined CSV is uploaded as a Delta table under `<company_id>/combined_financials/FINANCIAL_DATA_<timestamp>.parquet`.
  - **ZIP File Upload** (`/upload-zip`):
    - All data is consolidated into a single Parquet file at `<company_id>/consolidated_data.parquet`.
  - Metadata (e.g., `_metadata.json`) is generated for Power BI compatibility.
- **Location**: Azure Data Lake Storage Gen2, container `newcdcdata`.
- **Structure**:
  ```
  newcdcdata/
  ├── AEP/
  │   ├── Asset/
  │   │   ├── Revenue_20250426.parquet
  │   │   ├── Profit_20250426.parquet
  │   │   ├── asset_operational_20250426.parquet
  │   ├── combined_financials/
  │   │   ├── FINANCIAL_DATA_2025-04-26_12_00.parquet
  │   ├── consolidated_data.parquet
  │   ├── _metadata.json
  ```

### Step 3: View Data in Power BI
To visualize the uploaded data in Power BI, connect to Azure Data Lake Storage and query the Parquet/Delta files.

1. **Open Power BI Desktop**:
   - Download and install Power BI Desktop from [Microsoft’s website](https://powerbi.microsoft.com/desktop/).

2. **Connect to Azure Data Lake Storage**:
   - In Power BI, click **Get Data** > **Azure** > **Azure Data Lake Storage Gen2**.
   - Enter the **Account URL**: `https://<account-name>.dfs.core.windows.net` (e.g., `https://mystorageaccount.dfs.core.windows.net`).
   - Select **Organizational account** or **Account key** for authentication:
     - **Account key**: Use your Azure Storage account key from the Azure Portal.
     - **Organizational account**: Sign in with your Azure AD credentials (requires Storage Blob Data Contributor role).

3. **Select the Container and Files**:
   - Navigate to the `newcdcdata` container.
   - Choose files or folders:
     - For consolidated data: Select `<company_id>/consolidated_data.parquet` (e.g., `AEP/consolidated_data.parquet`).
     - For specific metrics: Select `<company_id>/<table_name>` (e.g., `AEP/Asset/Revenue_20250426.parquet`).
     - For Delta tables: Select `<company_id>/combined_financials` and load all Parquet files.
   - Click **Load** to import the data.

4. **Transform Data (Optional)**:
   - Use Power BI’s **Power Query Editor** to:
     - Filter rows (e.g., by date or company).
     - Rename columns for clarity.
     - Merge multiple Parquet files if needed.
   - Example: Combine all files in `AEP/combined_financials` for a unified view.

5. **Create Visualizations**:
   - Create charts, tables, or dashboards:
     - **Line Chart**: Plot `Revenue` over `Date`.
     - **Table**: Show `Asset` operational data (e.g., IDs, dates).
   - Use the `SECTOR` and `COMPANY` columns (added automatically) for filtering (e.g., `SECTOR = GAS UTILITY`, `COMPANY = AEP`).

6. **Publish to Power BI Service (Optional)**:
   - Click **Publish** in Power BI Desktop.
   - Sign in to Power BI Service and share the report with your team.
   - Schedule data refresh:
     - In Power BI Service, go to the dataset settings.
     - Configure a **Gateway** or use **DirectQuery** to refresh data from Azure Data Lake.

---

## Testing the Pipeline
To ensure the pipeline works correctly, test file uploads and Power BI connectivity using `curl` or **Postman**.

### Test File Upload with Curl
1. **Prepare a Test File**:
   - Create a CSV file (`test.csv`):
     ```
     Date,Revenue,Profit
     2023-01-01,1000,200
     2023-01-02,1200,250
     ```
   - Or create a ZIP file with an XLSX file containing similar data.

2. **Upload the File**:
   - For CSV:
     ```bash
     curl -X POST "http://20.9.138.28:8000/upload" \
          -F "company_id=AEP" \
          -F "table_type=Asset" \
          -F "file=@test.csv"
     ```
   - For ZIP:
     ```bash
     curl -X POST "http://20.9.138.28:8000/upload-zip" \
          -F "file=@test.zip" \
          -F "output_folder=."
     ```

3. **Verify in Azure Data Lake**:
   - Use Azure Storage Explorer or Azure Portal to check:
     - `newcdcdata/AEP/Asset` for individual metrics and operational data.
     - `newcdcdata/AEP/consolidated_data.parquet` for ZIP uploads.
     - `newcdcdata/AEP/combined_financials` for Delta tables.
   - Example command (Azure CLI):
     ```bash
     az storage fs file list --account-name <account-name> --file-system newcdcdata --path AEP
     ```

### Test File Upload with Postman
1. **Install Postman**:
   - Download and install Postman from [www.postman.com](https://www.postman.com/downloads/).

2. **Prepare a Test File**:
   - Create a CSV file (`test.csv`) or ZIP file as described above.

3. **Test the `/upload` Endpoint**:
   - Open Postman and create a new request.
   - Set the method to **POST** and the URL to `http://20.9.138.28:8000/upload`.
   - Go to the **Body** tab, select **form-data**.
   - Add the following key-value pairs:
     - Key: `company_id`, Type: Text, Value: `AEP`
     - Key: `table_type`, Type: Text, Value: `Asset`
     - Key: `file`, Type: File, Value: Click “Select Files” and choose `test.csv`
   - Click **Send**.
   - **Expected Response** (Status: 200 OK):
     ```json
     {
       "message": "File processed successfully",
       "file_id": "<unique-id>",
       "company_id": "AEP",
       "table_name": "Asset",
       "changes_detected": 2
     }
     ```

4. **Test the `/upload-zip` Endpoint**:
   - Create a new request in Postman.
   - Set the method to **POST** and the URL to `http://20.9.138.28:8000/upload-zip`.
   - Go to the **Body** tab, select **form-data**.
   - Add the following key-value pairs:
     - Key: `file`, Type: File, Value: Click “Select Files” and choose `test.zip`
     - Key: `output_folder`, Type: Text, Value: `.`
   - Click **Send**.
   - **Expected Response** (Status: 200 OK):
     ```json
     {
       "message": "ZIP file processed successfully. Data consolidated into single Parquet file.",
       "file_id": "<unique-id>",
       "company_id": "AEP",
       "table_name": "consolidated_data",
       "changes_detected": 100
     }
     ```

5. **Verify in Azure Data Lake**:
   - Check the same paths as in the `curl` test using Azure Storage Explorer, Azure Portal, or the Azure CLI command.

### Test Power BI Connectivity
1. **Connect to Azure Data Lake** in Power BI as described above.
2. **Load a File** (e.g., `AEP/consolidated_data.parquet`).
3. **Verify Data**:
   - Check that columns like `Date`, `Revenue`, `Profit`, `SECTOR`, and `COMPANY` are present.
   - Create a simple chart (e.g., `Revenue` vs. `Date`) to confirm data integrity.

---

## Troubleshooting
- **Upload Fails**:
  - Ensure the file format is correct (CSV, XLSX, DOCX, ZIP).
  - Check server logs: `tail -f logs/app.log`.
  - Verify API endpoint and credentials.
- **Azure Data Lake Files Missing**:
  - Confirm `AZURE_STORAGE_CONNECTION_STRING` is set correctly.
  - Check container permissions in Azure Portal.
- **Power BI Connection Issues**:
  - Validate Azure credentials and container URL.
  - Ensure Parquet files are not corrupted (try opening in Python with `pandas.read_parquet`).
---