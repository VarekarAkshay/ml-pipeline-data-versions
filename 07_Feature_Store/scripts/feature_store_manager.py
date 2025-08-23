import os
import pandas as pd
import numpy as np
import sqlite3
import logging
from datetime import datetime, timedelta
import json
import yaml
import pathlib
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
import uuid
import warnings
warnings.filterwarnings('ignore')

@dataclass
class FeatureMetadata:
    """Feature metadata structure"""
    feature_id: str
    name: str
    description: str
    feature_group: str
    data_type: str
    version: str
    source_table: str
    source_column: str
    creation_date: datetime
    last_updated: datetime
    created_by: str
    tags: List[str]
    statistics: Dict[str, Any] = None
    quality_metrics: Dict[str, float] = None
    validation_rules: Dict[str, Any] = None

class RelativePathFilter(logging.Filter):
    def __init__(self, base_path):
        super().__init__()
        self.base_path = pathlib.Path(base_path).resolve()

    def filter(self, record):
        try:
            path = pathlib.Path(record.pathname).resolve()
            relative_path = path.relative_to(self.base_path)
            record.relativepathname = str(relative_path)
        except Exception:
            record.relativepathname = record.pathname
        return True

def setup_logging(base_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = base_path / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"feature_store_{timestamp}.log"

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(relativepathname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    path_filter = RelativePathFilter(base_path)
    file_handler.addFilter(path_filter)
    stream_handler.addFilter(path_filter)

    return logger

def load_config(script_folder):
    config_path = script_folder.parent / 'config_step7.yaml'
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

class FeatureStore:
    def __init__(self, logger, script_folder, config):
        self.logger = logger
        self.script_folder = script_folder
        self.config = config
        self.base_folder = self.script_folder.parent.resolve()
        
        # Initialize directories
        self.feature_store_dir = self.base_folder / pathlib.Path(self.config['feature_store']['database_path']).parent
        self.feature_store_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir = self.base_folder / pathlib.Path(self.config['feature_store']['metadata_path']).parent
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = self.base_folder / self.config['feature_store']['cache_path']
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir = self.base_folder / self.config['output']['reports_dir']
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        # Feature store database path
        self.db_path = self.base_folder / self.config['feature_store']['database_path']
        self.metadata_path = self.base_folder / self.config['feature_store']['metadata_path']
        
        # Initialize feature metadata storage
        self.feature_metadata: Dict[str, FeatureMetadata] = {}
        
    def initialize_feature_store(self):
        """Initialize the feature store database and schema"""
        relative_db_path = os.path.relpath(str(self.db_path), str(self.base_folder))
        self.logger.info(f"Initializing feature store database at: {relative_db_path}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create feature store schema
        schema_commands = [
            # Feature groups table
            """
            CREATE TABLE IF NOT EXISTS feature_groups (
                group_id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                source_table TEXT,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # Features metadata table
            """
            CREATE TABLE IF NOT EXISTS features_metadata (
                feature_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                feature_group_id TEXT,
                data_type TEXT NOT NULL,
                version TEXT DEFAULT '1.0',
                source_table TEXT,
                source_column TEXT,
                creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT,
                tags TEXT,  -- JSON string
                statistics TEXT,  -- JSON string
                quality_metrics TEXT,  -- JSON string
                validation_rules TEXT,  -- JSON string
                is_active BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (feature_group_id) REFERENCES feature_groups(group_id)
            )
            """,
            
            # Online feature store (current feature values)
            """
            CREATE TABLE IF NOT EXISTS online_features (
                entity_id TEXT NOT NULL,
                feature_id TEXT NOT NULL,
                feature_value TEXT,  -- JSON serialized value
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (entity_id, feature_id),
                FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id)
            )
            """,
            
            # Offline feature store (historical feature values)
            """
            CREATE TABLE IF NOT EXISTS offline_features (
                entity_id TEXT NOT NULL,
                feature_id TEXT NOT NULL,
                feature_value TEXT,  -- JSON serialized value
                timestamp TIMESTAMP NOT NULL,
                ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (entity_id, feature_id, timestamp),
                FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id)
            )
            """,
            
            # Feature access logs
            """
            CREATE TABLE IF NOT EXISTS feature_access_logs (
                log_id TEXT PRIMARY KEY,
                feature_id TEXT,
                entity_id TEXT,
                access_type TEXT,  -- online, offline, batch
                access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                request_source TEXT,
                response_time_ms INTEGER,
                FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id)
            )
            """,
            
            # Data quality metrics
            """
            CREATE TABLE IF NOT EXISTS feature_quality_metrics (
                metric_id TEXT PRIMARY KEY,
                feature_id TEXT,
                metric_name TEXT,
                metric_value REAL,
                measurement_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (feature_id) REFERENCES features_metadata(feature_id)
            )
            """
        ]
        
        for command in schema_commands:
            cursor.execute(command)
            
        # Create indexes for performance
        index_commands = [
            "CREATE INDEX IF NOT EXISTS idx_features_name ON features_metadata(name)",
            "CREATE INDEX IF NOT EXISTS idx_features_group ON features_metadata(feature_group_id)",
            "CREATE INDEX IF NOT EXISTS idx_online_entity ON online_features(entity_id)",
            "CREATE INDEX IF NOT EXISTS idx_offline_entity_time ON offline_features(entity_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_access_logs_time ON feature_access_logs(access_time)",
            "CREATE INDEX IF NOT EXISTS idx_quality_metrics_time ON feature_quality_metrics(measurement_time)"
        ]
        
        for command in index_commands:
            cursor.execute(command)
            
        conn.commit()
        conn.close()
        self.logger.info("Feature store database initialized successfully")

    def connect_to_step6_datawarehouse(self):
        """Connect to Step 6 data warehouse"""
        step6_db_path = self.base_folder / self.config['input_sources']['step6_datawarehouse']
        relative_step6_path = os.path.relpath(str(step6_db_path), str(self.base_folder))
        self.logger.info(f"Connecting to Step 6 data warehouse: {relative_step6_path}")
        
        if not step6_db_path.exists():
            raise FileNotFoundError(f"Step 6 data warehouse not found at: {step6_db_path}")
            
        return sqlite3.connect(step6_db_path)

    def load_step6_metadata(self):
        """Load Step 6 transformation metadata for feature lineage"""
        step6_reports_dir = self.base_folder / self.config['input_sources']['step6_reports']
        pattern = self.config['input_sources']['step6_metadata_pattern']
        
        metadata_files = list(step6_reports_dir.glob(pattern))
        if not metadata_files:
            self.logger.warning(f"No Step 6 metadata files found matching pattern: {pattern}")
            return {}
            
        latest_metadata_file = max(metadata_files, key=lambda p: p.stat().st_mtime)
        relative_metadata_path = os.path.relpath(str(latest_metadata_file), str(self.base_folder))
        self.logger.info(f"Loading Step 6 metadata from: {relative_metadata_path}")
        
        with open(latest_metadata_file, 'r') as f:
            return json.load(f)

    def register_feature_groups(self):
        """Register feature groups from configuration"""
        self.logger.info("Registering feature groups...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for group_name, group_config in self.config['features'].items():
            group_id = f"fg_{group_name}"
            description = group_config.get('description', '')
            source_table = group_config.get('source_table', '')
            
            cursor.execute("""
                INSERT OR REPLACE INTO feature_groups 
                (group_id, name, description, source_table, updated_date)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (group_id, group_name, description, source_table))
            
        conn.commit()
        conn.close()
        
        self.logger.info(f"Registered {len(self.config['features'])} feature groups")

    def register_features(self):
        """Register individual features from configuration"""
        self.logger.info("Registering features...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        feature_count = 0
        
        for group_name, group_config in self.config['features'].items():
            group_id = f"fg_{group_name}"
            
            for feature_config in group_config.get('features', []):
                feature_id = f"{group_name}_{feature_config['name']}_v{feature_config['version']}"
                
                # Create feature metadata object
                metadata = FeatureMetadata(
                    feature_id=feature_id,
                    name=feature_config['name'],
                    description=feature_config.get('description', ''),
                    feature_group=group_name,
                    data_type=feature_config['type'],
                    version=feature_config['version'],
                    source_table=group_config.get('source_table', ''),
                    source_column=feature_config.get('source_column', ''),
                    creation_date=datetime.now(),
                    last_updated=datetime.now(),
                    created_by='feature_store_manager',
                    tags=feature_config.get('tags', [])
                )
                
                # Store in memory
                self.feature_metadata[feature_id] = metadata
                
                # Store in database
                cursor.execute("""
                    INSERT OR REPLACE INTO features_metadata 
                    (feature_id, name, description, feature_group_id, data_type, version,
                     source_table, source_column, creation_date, last_updated, created_by, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    feature_id, metadata.name, metadata.description, group_id,
                    metadata.data_type, metadata.version, metadata.source_table,
                    metadata.source_column, metadata.creation_date, metadata.last_updated,
                    metadata.created_by, json.dumps(metadata.tags)
                ))
                
                feature_count += 1
                
        conn.commit()
        conn.close()
        
        self.logger.info(f"Registered {feature_count} features")

    def ingest_features_from_step6(self):
        """Ingest feature values from Step 6 data warehouse"""
        self.logger.info("Ingesting features from Step 6 data warehouse...")
        
        step6_conn = self.connect_to_step6_datawarehouse()
        fs_conn = sqlite3.connect(self.db_path)
        
        # Get customer data with features
        query = """
            SELECT
                c.customer_id,
                c.geography,
                c.gender,
                c.age,
                c.age_group,
                f.balance_mean,
                f.balance_std,
                f.credit_score_mean,
                f.estimated_salary_mean,
                f.balance_to_salary_ratio,
                f.high_value_customer,
                f.geographic_risk_score,
                f.credit_score_category,
                f.last_updated
            FROM dim_customers c
            LEFT JOIN fact_customer_features f ON c.customer_id = f.customer_id
        """
        
        df = pd.read_sql_query(query, step6_conn)
        self.logger.info(f"Retrieved {len(df)} customer records from Step 6")
        
        # Ingest features into online and offline stores
        fs_cursor = fs_conn.cursor()
        
        ingested_count = 0
        timestamp = datetime.now()
        
        for _, row in df.iterrows():
            entity_id = str(row['customer_id'])
            
            # Map DataFrame columns to feature IDs
            feature_mappings = {
                'geography': 'customer_demographics_geography_mode_v1.0',
                'gender': 'customer_demographics_gender_mode_v1.0',
                'age': 'customer_demographics_age_mean_v1.0',
                'age_group': 'customer_demographics_age_group_v1.0',
                'balance_mean': 'customer_financial_balance_mean_v1.0',
                'balance_std': 'customer_financial_balance_std_v1.0',
                'credit_score_mean': 'customer_financial_credit_score_mean_v1.0',
                'estimated_salary_mean': 'customer_financial_estimated_salary_mean_v1.0',
                'balance_to_salary_ratio': 'customer_derived_balance_to_salary_ratio_v1.0',
                'high_value_customer': 'customer_derived_high_value_customer_v1.0',
                'geographic_risk_score': 'customer_derived_geographic_risk_score_v1.0',
                'credit_score_category': 'customer_derived_credit_score_category_v1.0'
            }
            
            for column, feature_id in feature_mappings.items():
                if column in row and pd.notna(row[column]):
                    feature_value = json.dumps(row[column])
                    
                    # Insert into online store
                    fs_cursor.execute("""
                        INSERT OR REPLACE INTO online_features 
                        (entity_id, feature_id, feature_value, last_updated)
                        VALUES (?, ?, ?, ?)
                    """, (entity_id, feature_id, feature_value, timestamp))
                    
                    # Insert into offline store
                    fs_cursor.execute("""
                        INSERT OR REPLACE INTO offline_features 
                        (entity_id, feature_id, feature_value, timestamp)
                        VALUES (?, ?, ?, ?)
                    """, (entity_id, feature_id, feature_value, timestamp))
                    
                    ingested_count += 1
        
        fs_conn.commit()
        step6_conn.close()
        fs_conn.close()
        
        self.logger.info(f"Ingested {ingested_count} feature values")

    def calculate_feature_statistics(self):
        """Calculate statistics for ingested features"""
        self.logger.info("Calculating feature statistics...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get numerical feature statistics
        for feature_id, metadata in self.feature_metadata.items():
            if metadata.data_type in ['float', 'integer']:
                query = """
                SELECT 
                    COUNT(*) as count,
                    MIN(CAST(JSON_EXTRACT(feature_value, '$') AS REAL)) as min_val,
                    MAX(CAST(JSON_EXTRACT(feature_value, '$') AS REAL)) as max_val,
                    AVG(CAST(JSON_EXTRACT(feature_value, '$') AS REAL)) as mean_val
                FROM online_features 
                WHERE feature_id = ?
                """
                
                result = pd.read_sql_query(query, conn, params=[feature_id])
                
                if len(result) > 0 and result.iloc[0]['count'] > 0:
                    stats = {
                        'count': int(result.iloc[0]['count']),
                        'min_value': float(result.iloc[0]['min_val']) if result.iloc[0]['min_val'] is not None else None,
                        'max_value': float(result.iloc[0]['max_val']) if result.iloc[0]['max_val'] is not None else None,
                        'mean_value': float(result.iloc[0]['mean_val']) if result.iloc[0]['mean_val'] is not None else None
                    }
                    
                    metadata.statistics = stats
        
        conn.close()
        self.logger.info("Feature statistics calculated")

    def save_metadata_to_file(self):
        """Save feature metadata to JSON file"""
        metadata_dict = {}
        
        for feature_id, metadata in self.feature_metadata.items():
            metadata_dict[feature_id] = {
                'feature_id': metadata.feature_id,
                'name': metadata.name,
                'description': metadata.description,
                'feature_group': metadata.feature_group,
                'data_type': metadata.data_type,
                'version': metadata.version,
                'source_table': metadata.source_table,
                'source_column': metadata.source_column,
                'creation_date': metadata.creation_date.isoformat(),
                'last_updated': metadata.last_updated.isoformat(),
                'created_by': metadata.created_by,
                'tags': metadata.tags,
                'statistics': metadata.statistics,
                'quality_metrics': metadata.quality_metrics,
                'validation_rules': metadata.validation_rules
            }
        
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata_dict, f, indent=2)
            
        relative_metadata_path = os.path.relpath(str(self.metadata_path), str(self.base_folder))
        self.logger.info(f"Feature metadata saved to: {relative_metadata_path}")

    def generate_feature_documentation(self):
        """Generate feature store documentation"""
        self.logger.info("Generating feature documentation...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        doc_path = self.reports_dir / f"feature_store_documentation_{timestamp}.md"
        
        with open(doc_path, 'w') as f:
            f.write("# Feature Store Documentation\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")
            
            # Summary
            f.write("## Summary\n\n")
            f.write(f"- Total Features: {len(self.feature_metadata)}\n")
            f.write(f"- Feature Groups: {len(self.config['features'])}\n\n")
            
            # Feature Groups
            for group_name, group_config in self.config['features'].items():
                f.write(f"## Feature Group: {group_name}\n\n")
                f.write(f"**Description**: {group_config.get('description', 'N/A')}\n\n")
                f.write(f"**Source Table**: {group_config.get('source_table', 'N/A')}\n\n")
                
                f.write("### Features\n\n")
                f.write("| Feature Name | Type | Description | Version | Tags |\n")
                f.write("|--------------|------|-------------|---------|------|\n")
                
                for feature_config in group_config.get('features', []):
                    name = feature_config['name']
                    dtype = feature_config['type']
                    desc = feature_config.get('description', '')
                    version = feature_config['version']
                    tags = ', '.join(feature_config.get('tags', []))
                    
                    f.write(f"| {name} | {dtype} | {desc} | {version} | {tags} |\n")
                
                f.write("\n")
        
        relative_doc_path = os.path.relpath(str(doc_path), str(self.base_folder))
        self.logger.info(f"Feature documentation generated: {relative_doc_path}")
        
        return doc_path

    def generate_feature_summary_report(self):
        """Generate feature store summary report"""
        self.logger.info("Generating feature store summary report...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create summary report
        summary = {
            'feature_store_summary': {
                'generation_timestamp': datetime.now().isoformat(),
                'total_features': len(self.feature_metadata),
                'total_feature_groups': len(self.config['features']),
                'feature_store_database': str(self.db_path),
                'step6_integration': {
                    'source_datawarehouse': self.config['input_sources']['step6_datawarehouse'],
                    'ingestion_enabled': self.config['ingestion']['auto_ingestion_enabled']
                }
            },
            'feature_groups': {},
            'feature_statistics': {},
            'configuration_summary': {
                'online_store_enabled': self.config['serving']['online_store_enabled'],
                'offline_store_enabled': self.config['serving']['offline_store_enabled'],
                'api_enabled': self.config['serving']['api_enabled'],
                'versioning_enabled': self.config['versioning']['auto_versioning'],
                'quality_monitoring': self.config['validation']['quality_monitoring_enabled']
            }
        }
        
        # Feature group summaries
        for group_name, group_config in self.config['features'].items():
            summary['feature_groups'][group_name] = {
                'description': group_config.get('description', ''),
                'source_table': group_config.get('source_table', ''),
                'feature_count': len(group_config.get('features', [])),
                'features': [f['name'] for f in group_config.get('features', [])]
            }
        
        # Feature statistics
        for feature_id, metadata in self.feature_metadata.items():
            if metadata.statistics:
                summary['feature_statistics'][feature_id] = metadata.statistics
        
        # Save summary report
        summary_path = self.reports_dir / f"feature_store_summary_{timestamp}.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        relative_summary_path = os.path.relpath(str(summary_path), str(self.base_folder))
        self.logger.info(f"Feature store summary report saved to: {relative_summary_path}")
        
        return summary_path

    def run(self):
        """Run the complete feature store setup and ingestion process"""
        try:
            self.logger.info("Starting feature store initialization...")
            
            # Initialize feature store
            self.initialize_feature_store()
            
            # Load Step 6 metadata for lineage
            step6_metadata = self.load_step6_metadata()
            
            # Register feature groups and features
            self.register_feature_groups()
            self.register_features()
            
            # Ingest features from Step 6
            self.ingest_features_from_step6()
            
            # Calculate statistics
            self.calculate_feature_statistics()
            
            # Save metadata
            self.save_metadata_to_file()
            
            # Generate documentation and reports
            doc_path = self.generate_feature_documentation()
            summary_path = self.generate_feature_summary_report()
            
            self.logger.info("Feature store initialization completed successfully")
            
            return {
                'feature_store_db': self.db_path,
                'metadata_file': self.metadata_path,
                'documentation': doc_path,
                'summary_report': summary_path,
                'total_features': len(self.feature_metadata)
            }
            
        except Exception as e:
            self.logger.error(f"Feature store initialization failed: {e}")
            raise

if __name__ == "__main__":
    script_folder = pathlib.Path(__file__).parent.resolve()
    project_root = script_folder.parent.parent.resolve()
    logger = setup_logging(script_folder.parent)
    config = load_config(script_folder)
    feature_store = FeatureStore(logger, script_folder, config)

    try:
        result = feature_store.run()
        # Compute relative paths against project root
        rel_db = os.path.relpath(str(result['feature_store_db']), str(project_root))
        rel_doc = os.path.relpath(str(result['documentation']), str(project_root))
        rel_summary = os.path.relpath(str(result['summary_report']), str(project_root))

        print("Feature store initialization completed successfully!")
        print(f"Feature store database: {rel_db}")
        print(f"Total features registered: {result['total_features']}")
        print(f"Documentation: {rel_doc}")
        print(f"Summary report: {rel_summary}")
    except Exception as e:
        print(f"Feature store initialization failed: {e}")
