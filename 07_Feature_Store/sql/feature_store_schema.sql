-- Feature Store Database Schema
-- Step 7: Feature Store Implementation
-- Created: 2025-08-23

-- =====================================
-- FEATURE MANAGEMENT TABLES
-- =====================================

-- Feature groups table
-- Organizes features into logical groups for better management
CREATE TABLE IF NOT EXISTS feature_groups (
    group_id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    source_table TEXT,
    source_system TEXT DEFAULT 'step6_datawarehouse',
    owner TEXT DEFAULT 'data_team',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Features metadata table
-- Central registry for all feature definitions and metadata
CREATE TABLE IF NOT EXISTS features_metadata (
    feature_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    feature_group_id TEXT NOT NULL,
    data_type TEXT NOT NULL CHECK(data_type IN ('string', 'integer', 'float', 'boolean', 'json')),
    version TEXT DEFAULT '1.0',
    source_table TEXT,
    source_column TEXT,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'system',
    owner TEXT DEFAULT 'data_team',
    
    -- Feature metadata as JSON
    tags TEXT,  -- JSON array of tags
    categories TEXT,  -- JSON array of valid categories for categorical features
    statistics TEXT,  -- JSON object with feature statistics
    quality_metrics TEXT,  -- JSON object with data quality metrics
    validation_rules TEXT,  -- JSON object with validation rules
    
    -- Feature lifecycle
    status TEXT DEFAULT 'active' CHECK(status IN ('active', 'deprecated', 'archived')),
    deprecation_date TIMESTAMP NULL,
    is_active BOOLEAN DEFAULT TRUE,
    
    FOREIGN KEY (feature_group_id) REFERENCES feature_groups(group_id) ON DELETE CASCADE
);

-- Feature versions table
-- Track feature evolution and backward compatibility
CREATE TABLE IF NOT EXISTS feature_versions (
    version_id TEXT PRIMARY KEY,
    feature_id TEXT NOT NULL,
    version TEXT NOT NULL,
    schema_definition TEXT,  -- JSON schema definition
    compatibility_notes TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    is_current BOOLEAN DEFAULT FALSE,
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE,
    UNIQUE(feature_id, version)
);

-- =====================================
-- FEATURE SERVING TABLES
-- =====================================

-- Online feature store
-- Stores current/latest feature values for real-time serving
CREATE TABLE IF NOT EXISTS online_features (
    entity_id TEXT NOT NULL,
    feature_id TEXT NOT NULL,
    feature_value TEXT NOT NULL,  -- JSON serialized feature value
    feature_timestamp TIMESTAMP,  -- When the feature was computed
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When stored in feature store
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ttl_expires TIMESTAMP,  -- Time-to-live expiration
    
    PRIMARY KEY (entity_id, feature_id),
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE
);

-- Offline feature store
-- Stores historical feature values for training and batch serving
CREATE TABLE IF NOT EXISTS offline_features (
    record_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    feature_id TEXT NOT NULL,
    feature_value TEXT NOT NULL,  -- JSON serialized feature value
    feature_timestamp TIMESTAMP NOT NULL,  -- Point-in-time when feature was valid
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id TEXT,  -- Batch identifier for bulk ingestion
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE
);

-- Feature snapshots
-- Periodic snapshots of feature states for backup and analysis
CREATE TABLE IF NOT EXISTS feature_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    feature_group_id TEXT NOT NULL,
    features_json TEXT NOT NULL,  -- JSON object with all features for the entity
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'system',
    
    FOREIGN KEY (feature_group_id) REFERENCES feature_groups(group_id)
);

-- =====================================
-- FEATURE LINEAGE AND DEPENDENCIES
-- =====================================

-- Feature lineage table
-- Track how features are derived from source data and other features
CREATE TABLE IF NOT EXISTS feature_lineage (
    lineage_id TEXT PRIMARY KEY,
    downstream_feature_id TEXT NOT NULL,  -- Feature that depends on others
    upstream_source_type TEXT NOT NULL CHECK(upstream_source_type IN ('table', 'feature', 'external')),
    upstream_source_name TEXT NOT NULL,   -- Name of upstream source
    upstream_source_column TEXT,          -- Column if source is table
    transformation_logic TEXT,            -- Description of transformation applied
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (downstream_feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE
);

-- Feature dependencies
-- Explicit dependencies between features
CREATE TABLE IF NOT EXISTS feature_dependencies (
    dependency_id TEXT PRIMARY KEY,
    parent_feature_id TEXT NOT NULL,
    child_feature_id TEXT NOT NULL,
    dependency_type TEXT DEFAULT 'computation' CHECK(dependency_type IN ('computation', 'validation', 'business')),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (parent_feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE,
    FOREIGN KEY (child_feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE,
    UNIQUE(parent_feature_id, child_feature_id)
);

-- =====================================
-- FEATURE ACCESS AND MONITORING
-- =====================================

-- Feature access logs
-- Track feature retrieval for monitoring and usage analytics
CREATE TABLE IF NOT EXISTS feature_access_logs (
    log_id TEXT PRIMARY KEY,
    feature_id TEXT,
    entity_id TEXT,
    access_type TEXT CHECK(access_type IN ('online', 'offline', 'batch', 'historical')),
    access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    request_source TEXT,  -- API, batch_job, notebook, etc.
    user_id TEXT,
    response_time_ms INTEGER,
    cache_hit BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id)
);

-- Feature usage statistics
-- Aggregated usage statistics for monitoring and optimization
CREATE TABLE IF NOT EXISTS feature_usage_stats (
    stat_id TEXT PRIMARY KEY,
    feature_id TEXT NOT NULL,
    date DATE NOT NULL,
    total_requests INTEGER DEFAULT 0,
    unique_entities INTEGER DEFAULT 0,
    avg_response_time_ms REAL DEFAULT 0,
    error_rate REAL DEFAULT 0,
    cache_hit_rate REAL DEFAULT 0,
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE,
    UNIQUE(feature_id, date)
);

-- =====================================
-- DATA QUALITY AND VALIDATION
-- =====================================

-- Feature quality metrics
-- Store data quality assessments for features
CREATE TABLE IF NOT EXISTS feature_quality_metrics (
    metric_id TEXT PRIMARY KEY,
    feature_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    metric_threshold REAL,
    status TEXT DEFAULT 'unknown' CHECK(status IN ('pass', 'fail', 'warning', 'unknown')),
    measurement_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    measurement_batch_id TEXT,
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE
);

-- Feature validation results
-- Results of feature validation checks
CREATE TABLE IF NOT EXISTS feature_validation_results (
    validation_id TEXT PRIMARY KEY,
    feature_id TEXT NOT NULL,
    validation_rule TEXT NOT NULL,
    validation_status TEXT CHECK(validation_status IN ('pass', 'fail', 'skip')),
    validation_message TEXT,
    records_validated INTEGER,
    records_failed INTEGER,
    validation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE
);

-- Data drift detection
-- Monitor feature distribution changes over time
CREATE TABLE IF NOT EXISTS feature_drift_detection (
    drift_id TEXT PRIMARY KEY,
    feature_id TEXT NOT NULL,
    baseline_period_start DATE,
    baseline_period_end DATE,
    comparison_period_start DATE,
    comparison_period_end DATE,
    drift_score REAL NOT NULL,
    drift_method TEXT DEFAULT 'ks_test',
    significance_level REAL DEFAULT 0.05,
    is_drift_detected BOOLEAN DEFAULT FALSE,
    detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id) ON DELETE CASCADE
);

-- =====================================
-- FEATURE STORE CONFIGURATION
-- =====================================

-- Feature store configuration
-- Store feature store settings and configurations
CREATE TABLE IF NOT EXISTS feature_store_config (
    config_id TEXT PRIMARY KEY,
    config_key TEXT UNIQUE NOT NULL,
    config_value TEXT NOT NULL,
    config_type TEXT DEFAULT 'string' CHECK(config_type IN ('string', 'integer', 'float', 'boolean', 'json')),
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT
);

-- Feature store operations log
-- Log major feature store operations
CREATE TABLE IF NOT EXISTS feature_store_operations (
    operation_id TEXT PRIMARY KEY,
    operation_type TEXT NOT NULL CHECK(operation_type IN ('ingestion', 'refresh', 'backup', 'migration', 'cleanup')),
    operation_status TEXT DEFAULT 'running' CHECK(operation_status IN ('running', 'completed', 'failed', 'cancelled')),
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds REAL,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata TEXT,  -- JSON with operation-specific metadata
    triggered_by TEXT DEFAULT 'system'
);

-- =====================================
-- INDEXES FOR PERFORMANCE
-- =====================================

-- Feature metadata indexes
CREATE INDEX IF NOT EXISTS idx_features_name ON features_metadata(name);
CREATE INDEX IF NOT EXISTS idx_features_group ON features_metadata(feature_group_id);
CREATE INDEX IF NOT EXISTS idx_features_status ON features_metadata(status);
CREATE INDEX IF NOT EXISTS idx_features_active ON features_metadata(is_active);
CREATE INDEX IF NOT EXISTS idx_features_updated ON features_metadata(last_updated);

-- Feature serving indexes
CREATE INDEX IF NOT EXISTS idx_online_entity ON online_features(entity_id);
CREATE INDEX IF NOT EXISTS idx_online_feature ON online_features(feature_id);
CREATE INDEX IF NOT EXISTS idx_online_updated ON online_features(last_updated);
CREATE INDEX IF NOT EXISTS idx_online_ttl ON online_features(ttl_expires);

CREATE INDEX IF NOT EXISTS idx_offline_entity ON offline_features(entity_id);
CREATE INDEX IF NOT EXISTS idx_offline_feature ON offline_features(feature_id);
CREATE INDEX IF NOT EXISTS idx_offline_timestamp ON offline_features(feature_timestamp);
CREATE INDEX IF NOT EXISTS idx_offline_batch ON offline_features(batch_id);
CREATE INDEX IF NOT EXISTS idx_offline_entity_time ON offline_features(entity_id, feature_timestamp);

-- Access and monitoring indexes
CREATE INDEX IF NOT EXISTS idx_access_logs_time ON feature_access_logs(access_time);
CREATE INDEX IF NOT EXISTS idx_access_logs_feature ON feature_access_logs(feature_id);
CREATE INDEX IF NOT EXISTS idx_access_logs_entity ON feature_access_logs(entity_id);
CREATE INDEX IF NOT EXISTS idx_access_logs_type ON feature_access_logs(access_type);

CREATE INDEX IF NOT EXISTS idx_usage_stats_date ON feature_usage_stats(date);
CREATE INDEX IF NOT EXISTS idx_usage_stats_feature ON feature_usage_stats(feature_id);

-- Quality and validation indexes
CREATE INDEX IF NOT EXISTS idx_quality_feature ON feature_quality_metrics(feature_id);
CREATE INDEX IF NOT EXISTS idx_quality_time ON feature_quality_metrics(measurement_time);
CREATE INDEX IF NOT EXISTS idx_quality_status ON feature_quality_metrics(status);

CREATE INDEX IF NOT EXISTS idx_validation_feature ON feature_validation_results(feature_id);
CREATE INDEX IF NOT EXISTS idx_validation_time ON feature_validation_results(validation_time);
CREATE INDEX IF NOT EXISTS idx_validation_status ON feature_validation_results(validation_status);

CREATE INDEX IF NOT EXISTS idx_drift_feature ON feature_drift_detection(feature_id);
CREATE INDEX IF NOT EXISTS idx_drift_time ON feature_drift_detection(detection_time);
CREATE INDEX IF NOT EXISTS idx_drift_detected ON feature_drift_detection(is_drift_detected);

-- Lineage indexes
CREATE INDEX IF NOT EXISTS idx_lineage_downstream ON feature_lineage(downstream_feature_id);
CREATE INDEX IF NOT EXISTS idx_lineage_upstream ON feature_lineage(upstream_source_name);

CREATE INDEX IF NOT EXISTS idx_dependencies_parent ON feature_dependencies(parent_feature_id);
CREATE INDEX IF NOT EXISTS idx_dependencies_child ON feature_dependencies(child_feature_id);

-- =====================================
-- VIEWS FOR COMMON QUERIES
-- =====================================

-- Feature catalog view
-- Comprehensive view of all features with metadata
CREATE VIEW IF NOT EXISTS view_feature_catalog AS
SELECT 
    f.feature_id,
    f.name,
    f.description,
    f.data_type,
    f.version,
    f.status,
    fg.name as feature_group_name,
    fg.description as feature_group_description,
    f.source_table,
    f.source_column,
    f.tags,
    f.creation_date,
    f.last_updated,
    f.created_by,
    f.owner
FROM features_metadata f
JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE
ORDER BY fg.name, f.name;

-- Feature usage summary view
-- Summary of feature usage patterns
CREATE VIEW IF NOT EXISTS view_feature_usage_summary AS
SELECT 
    f.feature_id,
    f.name as feature_name,
    fg.name as feature_group,
    COUNT(DISTINCT al.entity_id) as unique_entities_accessed,
    COUNT(al.log_id) as total_requests,
    AVG(al.response_time_ms) as avg_response_time_ms,
    MAX(al.access_time) as last_accessed,
    COUNT(CASE WHEN al.access_time >= datetime('now', '-7 days') THEN 1 END) as requests_last_7_days,
    COUNT(CASE WHEN al.access_time >= datetime('now', '-30 days') THEN 1 END) as requests_last_30_days
FROM features_metadata f
LEFT JOIN feature_access_logs al ON f.feature_id = al.feature_id
LEFT JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE
GROUP BY f.feature_id, f.name, fg.name;

-- Feature quality overview
-- Overview of feature quality status
CREATE VIEW IF NOT EXISTS view_feature_quality_overview AS
SELECT 
    f.feature_id,
    f.name as feature_name,
    fg.name as feature_group,
    COUNT(CASE WHEN fqm.status = 'pass' THEN 1 END) as quality_checks_passed,
    COUNT(CASE WHEN fqm.status = 'fail' THEN 1 END) as quality_checks_failed,
    COUNT(CASE WHEN fqm.status = 'warning' THEN 1 END) as quality_checks_warning,
    MAX(fqm.measurement_time) as last_quality_check,
    CASE 
        WHEN COUNT(CASE WHEN fqm.status = 'fail' THEN 1 END) > 0 THEN 'Poor'
        WHEN COUNT(CASE WHEN fqm.status = 'warning' THEN 1 END) > 0 THEN 'Fair'
        WHEN COUNT(CASE WHEN fqm.status = 'pass' THEN 1 END) > 0 THEN 'Good'
        ELSE 'Unknown'
    END as overall_quality_status
FROM features_metadata f
LEFT JOIN feature_quality_metrics fqm ON f.feature_id = fqm.feature_id
LEFT JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE
GROUP BY f.feature_id, f.name, fg.name;

-- Online features availability view
-- Check which entities have features available online
CREATE VIEW IF NOT EXISTS view_online_features_availability AS
SELECT 
    entity_id,
    COUNT(DISTINCT feature_id) as features_available,
    COUNT(CASE WHEN ttl_expires IS NULL OR ttl_expires > datetime('now') THEN 1 END) as features_valid,
    MAX(last_updated) as last_feature_update
FROM online_features
GROUP BY entity_id;

-- Feature lineage view
-- Complete feature lineage information
CREATE VIEW IF NOT EXISTS view_feature_lineage AS
SELECT 
    f.feature_id,
    f.name as feature_name,
    fg.name as feature_group,
    fl.upstream_source_type,
    fl.upstream_source_name,
    fl.upstream_source_column,
    fl.transformation_logic,
    fl.created_date as lineage_created_date
FROM features_metadata f
LEFT JOIN feature_lineage fl ON f.feature_id = fl.downstream_feature_id
LEFT JOIN feature_groups fg ON f.feature_group_id = fg.group_id
WHERE f.is_active = TRUE;

-- =====================================
-- TRIGGERS FOR DATA INTEGRITY
-- =====================================

-- Update timestamp trigger for features_metadata
CREATE TRIGGER IF NOT EXISTS trigger_features_metadata_updated
AFTER UPDATE ON features_metadata
FOR EACH ROW
BEGIN
    UPDATE features_metadata 
    SET last_updated = CURRENT_TIMESTAMP 
    WHERE feature_id = NEW.feature_id;
END;

-- Update timestamp trigger for feature_groups
CREATE TRIGGER IF NOT EXISTS trigger_feature_groups_updated
AFTER UPDATE ON feature_groups
FOR EACH ROW
BEGIN
    UPDATE feature_groups 
    SET updated_date = CURRENT_TIMESTAMP 
    WHERE group_id = NEW.group_id;
END;

-- Automatic feature version creation trigger
CREATE TRIGGER IF NOT EXISTS trigger_create_feature_version
AFTER INSERT ON features_metadata
FOR EACH ROW
BEGIN
    INSERT INTO feature_versions (
        version_id, feature_id, version, created_date, created_by, is_current
    ) VALUES (
        NEW.feature_id || '_' || NEW.version,
        NEW.feature_id,
        NEW.version,
        CURRENT_TIMESTAMP,
        NEW.created_by,
        TRUE
    );
END;

-- =====================================
-- INITIAL CONFIGURATION DATA
-- =====================================

-- Insert default feature store configuration
INSERT OR IGNORE INTO feature_store_config (config_id, config_key, config_value, config_type, description) VALUES
('cfg_retention_days', 'feature_retention_days', '365', 'integer', 'Days to retain historical feature data'),
('cfg_cache_ttl', 'default_cache_ttl_hours', '24', 'integer', 'Default TTL for cached features in hours'),
('cfg_batch_size', 'ingestion_batch_size', '1000', 'integer', 'Default batch size for feature ingestion'),
('cfg_quality_threshold', 'quality_score_threshold', '0.85', 'float', 'Minimum quality score threshold'),
('cfg_drift_threshold', 'drift_detection_threshold', '0.1', 'float', 'Threshold for drift detection');

-- =====================================
-- SCHEMA VALIDATION AND CONSTRAINTS
-- =====================================

-- Enable foreign keys and other pragmas for data integrity
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;