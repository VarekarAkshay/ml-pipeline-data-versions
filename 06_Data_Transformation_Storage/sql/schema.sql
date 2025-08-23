-- Customer Data Warehouse Schema
-- Step 6: Data Transformation & Storage
-- Created: 2025-08-23

-- =====================================
-- DIMENSION TABLES
-- =====================================

-- Customer dimension table
-- Stores core customer demographic information
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_num INTEGER UNIQUE,
    geography TEXT NOT NULL CHECK(geography IN ('France', 'Germany', 'Spain')),
    gender TEXT NOT NULL CHECK(gender IN ('Male', 'Female')),
    age REAL CHECK(age > 0 AND age < 150),
    age_group TEXT CHECK(age_group IN ('Young', 'Middle-aged', 'Senior', 'Elderly')),
    tenure_days REAL DEFAULT 0,
    credit_score_category TEXT CHECK(credit_score_category IN ('Poor', 'Fair', 'Good', 'Very Good', 'Excellent')),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geography dimension for risk and regional analysis
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_id INTEGER PRIMARY KEY AUTOINCREMENT,
    geography_name TEXT UNIQUE NOT NULL,
    region TEXT,
    country_code TEXT,
    risk_score REAL DEFAULT 1.0,
    population INTEGER,
    economic_indicator REAL
);

-- Time dimension for temporal analysis
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    week INTEGER,
    day_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- =====================================
-- FACT TABLES
-- =====================================

-- Customer features fact table
-- Stores all engineered features for each customer
CREATE TABLE IF NOT EXISTS fact_customer_features (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    
    -- Original aggregated features
    balance_mean REAL DEFAULT 0,
    balance_sum REAL DEFAULT 0,
    balance_std REAL DEFAULT 0,
    balance_min REAL DEFAULT 0,
    balance_max REAL DEFAULT 0,
    
    credit_score_mean REAL DEFAULT 0,
    credit_score_std REAL DEFAULT 0,
    
    estimated_salary_mean REAL DEFAULT 0,
    estimated_salary_std REAL DEFAULT 0,
    
    age_mean REAL DEFAULT 0,
    
    -- Derived features
    balance_to_salary_ratio REAL DEFAULT 0,
    high_value_customer BOOLEAN DEFAULT FALSE,
    geographic_risk_score REAL DEFAULT 1.0,
    
    -- Customer behavior features
    spending_velocity REAL DEFAULT 0,
    account_stability_score REAL DEFAULT 0,
    risk_indicator REAL DEFAULT 0,
    
    -- Feature quality metrics
    feature_completeness REAL DEFAULT 1.0,
    data_quality_score REAL DEFAULT 1.0,
    
    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_date DATE DEFAULT (DATE('now')),
    
    -- Foreign key constraints
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id) ON DELETE CASCADE
);

-- Customer activity fact table
-- Stores time-series customer activity patterns
CREATE TABLE IF NOT EXISTS fact_customer_activity (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    activity_date DATE NOT NULL,
    
    -- Activity metrics
    transaction_count INTEGER DEFAULT 0,
    transaction_amount REAL DEFAULT 0,
    balance_change REAL DEFAULT 0,
    
    -- Temporal features
    days_since_last_activity INTEGER DEFAULT 0,
    monthly_activity_score REAL DEFAULT 0,
    seasonal_pattern_score REAL DEFAULT 0,
    
    -- Activity flags
    is_first_transaction BOOLEAN DEFAULT FALSE,
    is_large_transaction BOOLEAN DEFAULT FALSE,
    is_unusual_activity BOOLEAN DEFAULT FALSE,
    
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id) ON DELETE CASCADE
);

-- =====================================
-- METADATA TABLES
-- =====================================

-- Transformation metadata
-- Tracks all data transformation operations
CREATE TABLE IF NOT EXISTS transformation_metadata (
    transformation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    transformation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transformation_type TEXT NOT NULL,
    
    -- Process metrics
    input_records INTEGER DEFAULT 0,
    output_records INTEGER DEFAULT 0,
    features_created INTEGER DEFAULT 0,
    processing_time_seconds REAL DEFAULT 0,
    
    -- Configuration and parameters
    transformation_config TEXT, -- JSON string of configuration
    step5_input_file TEXT,
    output_files TEXT, -- JSON string of output file paths
    
    -- Quality metrics
    data_quality_score REAL DEFAULT 1.0,
    completeness_score REAL DEFAULT 1.0,
    consistency_score REAL DEFAULT 1.0,
    
    -- Status tracking
    status TEXT DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    error_message TEXT,
    
    created_by TEXT DEFAULT 'data_transformation_pipeline'
);

-- Feature metadata
-- Tracks information about each engineered feature
CREATE TABLE IF NOT EXISTS feature_metadata (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_name TEXT UNIQUE NOT NULL,
    feature_type TEXT NOT NULL CHECK(feature_type IN ('numerical', 'categorical', 'boolean', 'derived')),
    
    -- Feature description
    description TEXT,
    formula TEXT,
    data_type TEXT,
    
    -- Feature statistics
    min_value REAL,
    max_value REAL,
    mean_value REAL,
    std_value REAL,
    null_count INTEGER DEFAULT 0,
    unique_count INTEGER DEFAULT 0,
    
    -- Feature quality
    importance_score REAL DEFAULT 0,
    correlation_with_target REAL DEFAULT 0,
    information_gain REAL DEFAULT 0,
    
    -- Versioning
    version INTEGER DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Data quality metrics
-- Stores data quality assessment results
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    quality_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    column_name TEXT,
    
    -- Quality dimensions
    completeness REAL DEFAULT 0, -- % of non-null values
    accuracy REAL DEFAULT 0,     -- % of accurate values
    consistency REAL DEFAULT 0,  -- % of consistent values
    validity REAL DEFAULT 0,     -- % of values within valid ranges
    uniqueness REAL DEFAULT 0,   -- % of unique values where expected
    
    -- Specific metrics
    null_count INTEGER DEFAULT 0,
    duplicate_count INTEGER DEFAULT 0,
    outlier_count INTEGER DEFAULT 0,
    invalid_count INTEGER DEFAULT 0,
    
    -- Overall scores
    overall_quality_score REAL DEFAULT 0,
    
    measurement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    measurement_by TEXT DEFAULT 'automated_quality_check'
);

-- =====================================
-- INDEXES FOR PERFORMANCE
-- =====================================

-- Customer dimension indexes
CREATE INDEX IF NOT EXISTS idx_customers_geography ON dim_customers(geography);
CREATE INDEX IF NOT EXISTS idx_customers_gender ON dim_customers(gender);
CREATE INDEX IF NOT EXISTS idx_customers_age_group ON dim_customers(age_group);
CREATE INDEX IF NOT EXISTS idx_customers_created_date ON dim_customers(created_date);

-- Customer features fact indexes
CREATE INDEX IF NOT EXISTS idx_features_customer_id ON fact_customer_features(customer_id);
CREATE INDEX IF NOT EXISTS idx_features_last_updated ON fact_customer_features(last_updated);
CREATE INDEX IF NOT EXISTS idx_features_processing_date ON fact_customer_features(processing_date);
CREATE INDEX IF NOT EXISTS idx_features_high_value ON fact_customer_features(high_value_customer);
CREATE INDEX IF NOT EXISTS idx_features_risk_score ON fact_customer_features(geographic_risk_score);

-- Customer activity fact indexes
CREATE INDEX IF NOT EXISTS idx_activity_customer_id ON fact_customer_activity(customer_id);
CREATE INDEX IF NOT EXISTS idx_activity_date ON fact_customer_activity(activity_date);
CREATE INDEX IF NOT EXISTS idx_activity_customer_date ON fact_customer_activity(customer_id, activity_date);

-- Metadata indexes
CREATE INDEX IF NOT EXISTS idx_transformation_date ON transformation_metadata(transformation_date);
CREATE INDEX IF NOT EXISTS idx_transformation_type ON transformation_metadata(transformation_type);
CREATE INDEX IF NOT EXISTS idx_transformation_status ON transformation_metadata(status);

CREATE INDEX IF NOT EXISTS idx_feature_name ON feature_metadata(feature_name);
CREATE INDEX IF NOT EXISTS idx_feature_type ON feature_metadata(feature_type);
CREATE INDEX IF NOT EXISTS idx_feature_active ON feature_metadata(is_active);

CREATE INDEX IF NOT EXISTS idx_quality_table ON data_quality_metrics(table_name);
CREATE INDEX IF NOT EXISTS idx_quality_date ON data_quality_metrics(measurement_date);

-- =====================================
-- VIEWS FOR COMMON QUERIES
-- =====================================

-- Customer summary view
-- Combines customer information with their latest features
CREATE VIEW IF NOT EXISTS view_customer_summary AS
SELECT 
    c.customer_id,
    c.customer_num,
    c.geography,
    c.gender,
    c.age,
    c.age_group,
    c.credit_score_category,
    f.balance_mean,
    f.credit_score_mean,
    f.estimated_salary_mean,
    f.balance_to_salary_ratio,
    f.high_value_customer,
    f.geographic_risk_score,
    f.last_updated
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id;

-- High value customers view
CREATE VIEW IF NOT EXISTS view_high_value_customers AS
SELECT 
    c.customer_id,
    c.geography,
    c.gender,
    f.balance_mean,
    f.credit_score_mean,
    f.balance_to_salary_ratio,
    f.geographic_risk_score
FROM dim_customers c
JOIN fact_customer_features f ON c.customer_id = f.customer_id
WHERE f.high_value_customer = 1
ORDER BY f.balance_mean DESC;

-- Geographic analysis view
CREATE VIEW IF NOT EXISTS view_geographic_analysis AS
SELECT 
    c.geography,
    COUNT(*) as customer_count,
    AVG(f.balance_mean) as avg_balance,
    AVG(f.credit_score_mean) as avg_credit_score,
    AVG(f.balance_to_salary_ratio) as avg_balance_salary_ratio,
    COUNT(CASE WHEN f.high_value_customer = 1 THEN 1 END) as high_value_count,
    AVG(f.geographic_risk_score) as avg_risk_score
FROM dim_customers c
LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
GROUP BY c.geography;

-- Data quality summary view
CREATE VIEW IF NOT EXISTS view_data_quality_summary AS
SELECT 
    table_name,
    AVG(completeness) as avg_completeness,
    AVG(accuracy) as avg_accuracy,
    AVG(consistency) as avg_consistency,
    AVG(validity) as avg_validity,
    AVG(overall_quality_score) as avg_quality_score,
    MAX(measurement_date) as last_measured
FROM data_quality_metrics
GROUP BY table_name;

-- =====================================
-- TRIGGERS FOR DATA INTEGRITY
-- =====================================

-- Update timestamp trigger for customers
CREATE TRIGGER IF NOT EXISTS trigger_customers_updated
AFTER UPDATE ON dim_customers
FOR EACH ROW
BEGIN
    UPDATE dim_customers 
    SET updated_date = CURRENT_TIMESTAMP 
    WHERE customer_id = NEW.customer_id;
END;

-- Automatic data quality scoring trigger
CREATE TRIGGER IF NOT EXISTS trigger_feature_quality_update
AFTER INSERT ON fact_customer_features
FOR EACH ROW
BEGIN
    -- Calculate and update feature completeness
    UPDATE fact_customer_features
    SET feature_completeness = CASE 
        WHEN NEW.balance_mean IS NOT NULL 
         AND NEW.credit_score_mean IS NOT NULL 
         AND NEW.estimated_salary_mean IS NOT NULL 
        THEN 1.0 
        ELSE 0.7 
    END,
    data_quality_score = CASE
        WHEN NEW.balance_mean >= 0 
         AND NEW.credit_score_mean BETWEEN 300 AND 850
         AND NEW.estimated_salary_mean >= 0
        THEN 1.0
        ELSE 0.8
    END
    WHERE feature_id = NEW.feature_id;
END;

-- =====================================
-- INITIAL DATA POPULATION
-- =====================================

-- Insert default geography data
INSERT OR IGNORE INTO dim_geography (geography_name, region, country_code, risk_score) VALUES
('France', 'Western Europe', 'FR', 1.0),
('Germany', 'Central Europe', 'DE', 0.8),
('Spain', 'Southern Europe', 'ES', 1.2);

-- Insert feature metadata for tracking
INSERT OR IGNORE INTO feature_metadata (feature_name, feature_type, description) VALUES
('balance_mean', 'numerical', 'Average account balance per customer'),
('credit_score_mean', 'numerical', 'Average credit score per customer'),
('estimated_salary_mean', 'numerical', 'Average estimated salary per customer'),
('balance_to_salary_ratio', 'derived', 'Ratio of balance to estimated salary'),
('high_value_customer', 'boolean', 'Flag indicating high-value customer status'),
('geographic_risk_score', 'derived', 'Risk score based on geographic location'),
('age_group', 'categorical', 'Customer age category');

-- =====================================
-- SCHEMA VALIDATION
-- =====================================

-- Pragma settings for data integrity
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;