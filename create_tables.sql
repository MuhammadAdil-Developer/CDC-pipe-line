-- Create the companies table
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the cdc_events table
CREATE TABLE IF NOT EXISTS cdc_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    company_id VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    key_column VARCHAR(100) NOT NULL,
    key_value VARCHAR(255) NOT NULL,
    old_values JSONB,
    new_values JSONB,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_events_company_id ON cdc_events(company_id);
CREATE INDEX idx_cdc_events_table_name ON cdc_events(table_name);
CREATE INDEX idx_cdc_events_processed ON cdc_events(processed);

-- Create the company_data table
CREATE TABLE IF NOT EXISTS company_data (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    record_key VARCHAR(255) NOT NULL,
    data JSONB NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NULL,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (company_id, table_name, record_key, valid_from)
);

CREATE INDEX idx_company_data_lookup ON company_data(company_id, table_name, record_key);
CREATE INDEX idx_company_data_current ON company_data(is_current);