import os
import pandas as pd
import numpy as np
import sqlite3
import logging
from datetime import datetime
import json
import yaml
import pathlib
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder
from sklearn.model_selection import train_test_split
import warnings
warnings.filterwarnings('ignore')

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
    log_file = log_dir / f"data_transformation_{timestamp}.log"

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
    config_path = script_folder.parent / 'config_step6.yaml'
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

class DataTransformationManager:
    def __init__(self, logger, script_folder, config):
        self.logger = logger
        self.script_folder = script_folder
        self.config = config
        self.base_folder = self.script_folder.parent.resolve()  # 06_Data_Transformation_Storage folder
        self.output_dir = self.base_folder / self.config['output']['reports_dir']
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_dir = self.base_folder / pathlib.Path(self.config['database']['db_path']).parent
        self.db_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize scalers
        self.scalers = {
            'standard': StandardScaler(),
            'minmax': MinMaxScaler(),
            'robust': RobustScaler()
        }

    def get_latest_step5_output(self):
        step5_dir = (self.base_folder / self.config['input_data']['step5_output_dir']).resolve()
        relative_step5_dir = os.path.relpath(str(step5_dir), str(self.base_folder))
        self.logger.info(f"Checking Step 5 outputs in: {relative_step5_dir}")
        
        if not step5_dir.exists():
            raise FileNotFoundError(f"Step 5 output directory not found: {step5_dir}")

        # Find latest cleaned data file
        cleaned_files = list(step5_dir.glob("cleaned_data_*.csv"))
        if not cleaned_files:
            raise FileNotFoundError(f"No cleaned data files found in {step5_dir}")
        
        latest_data_file = max(cleaned_files, key=lambda p: p.stat().st_mtime)
        
        # Find corresponding metadata file
        timestamp = latest_data_file.stem.split('_')[-2] + '_' + latest_data_file.stem.split('_')[-1]
        metadata_file = step5_dir / f"processing_metadata_{timestamp}.json"
        
        if not metadata_file.exists():
            # Try to find any metadata file
            metadata_files = list(step5_dir.glob("processing_metadata_*.json"))
            if metadata_files:
                metadata_file = max(metadata_files, key=lambda p: p.stat().st_mtime)
            else:
                self.logger.warning("No metadata file found, proceeding without metadata")
                metadata_file = None

        relative_data_path = os.path.relpath(str(latest_data_file), str(self.base_folder))
        self.logger.info(f"Loading cleaned data from: {relative_data_path}")
        
        return latest_data_file, metadata_file

    def load_data(self):
        data_file, metadata_file = self.get_latest_step5_output()
        
        # Load the cleaned dataset
        df = pd.read_csv(data_file)
        self.logger.info(f"Loaded dataset with shape: {df.shape}")
        
        # Load metadata if available
        metadata = None
        if metadata_file and metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            self.logger.info(f"Loaded metadata with {len(metadata)} entries")
        
        return df, metadata

    def create_aggregated_features(self, df):
        self.logger.info("Creating aggregated features...")
        
        # Get ID columns that should not be aggregated
        id_cols = self.config['input_data']['id_columns']
        
        # Create customer-level aggregations
        aggregated_features = pd.DataFrame()
        
        # Start with customer IDs
        if 'CustomerId' in df.columns:
            aggregated_features['CustomerId'] = df['CustomerId'].unique()
        elif 'CustomerNum' in df.columns:
            aggregated_features['CustomerId'] = df['CustomerNum'].unique()
        
        # Group by customer for aggregations
        if 'CustomerId' in df.columns:
            grouped = df.groupby('CustomerId')
        elif 'CustomerNum' in df.columns:
            grouped = df.groupby('CustomerNum')
            aggregated_features = aggregated_features.rename(columns={'CustomerId': 'CustomerNum'})
        else:
            self.logger.warning("No customer ID column found, using row-level transformations")
            return df

        # Numerical aggregations
        for feature_config in self.config['feature_engineering']['customer_aggregations']['numerical_features']:
            feature_name = feature_config['feature']
            if feature_name in df.columns:
                for agg_func in feature_config['aggregations']:
                    new_col_name = f"{feature_name}_{agg_func}"
                    if agg_func == 'sum':
                        aggregated_features[new_col_name] = grouped[feature_name].sum().values
                    elif agg_func == 'mean':
                        aggregated_features[new_col_name] = grouped[feature_name].mean().values
                    elif agg_func == 'std':
                        aggregated_features[new_col_name] = grouped[feature_name].std().fillna(0).values
                    elif agg_func == 'min':
                        aggregated_features[new_col_name] = grouped[feature_name].min().values
                    elif agg_func == 'max':
                        aggregated_features[new_col_name] = grouped[feature_name].max().values
                        
        # Categorical aggregations
        for feature_config in self.config['feature_engineering']['customer_aggregations']['categorical_features']:
            feature_name = feature_config['feature']
            if feature_name in df.columns:
                for agg_func in feature_config['aggregations']:
                    if agg_func == 'mode':
                        new_col_name = f"{feature_name}_mode"
                        aggregated_features[new_col_name] = grouped[feature_name].agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else np.nan).values
                    elif agg_func == 'count_unique':
                        new_col_name = f"{feature_name}_unique_count"
                        aggregated_features[new_col_name] = grouped[feature_name].nunique().values

        self.logger.info(f"Created aggregated dataset with shape: {aggregated_features.shape}")
        return aggregated_features

    def create_derived_features(self, df):
        self.logger.info("Creating derived features...")
        
        derived_df = df.copy()
        
        for feature_config in self.config['feature_engineering']['derived_features']:
            feature_name = feature_config['name']
            
            if feature_name == "balance_to_salary_ratio":
                if 'Balance_mean' in df.columns and 'EstimatedSalary_mean' in df.columns:
                    derived_df[feature_name] = df['Balance_mean'] / (df['EstimatedSalary_mean'] + 1e-6)  # Avoid division by zero
                elif 'Balance' in df.columns and 'EstimatedSalary' in df.columns:
                    derived_df[feature_name] = df['Balance'] / (df['EstimatedSalary'] + 1e-6)
                    
            elif feature_name == "credit_score_category":
                credit_col = 'CreditScore_mean' if 'CreditScore_mean' in df.columns else 'CreditScore'
                if credit_col in df.columns:
                    bins = feature_config['bins']
                    labels = feature_config['labels']
                    derived_df[feature_name] = pd.cut(df[credit_col], bins=bins, labels=labels, right=False)
                    
            elif feature_name == "age_group":
                age_col = 'Age_mean' if 'Age_mean' in df.columns else 'Age'
                if age_col in df.columns:
                    bins = feature_config['bins']
                    labels = feature_config['labels']
                    derived_df[feature_name] = pd.cut(df[age_col], bins=bins, labels=labels, right=False)
                    
            elif feature_name == "high_value_customer":
                balance_col = 'Balance_mean' if 'Balance_mean' in df.columns else 'Balance'
                if balance_col in df.columns:
                    balance_p75 = df[balance_col].quantile(0.75)
                    derived_df[feature_name] = (df[balance_col] > balance_p75).astype(int)
                    
            elif feature_name == "geographic_risk_score":
                geo_col = 'Geography_mode' if 'Geography_mode' in df.columns else 'Geography'
                if geo_col in df.columns:
                    # Simple geographic risk mapping (can be made more sophisticated)
                    risk_mapping = {'France': 1, 'Germany': 2, 'Spain': 3}
                    derived_df[feature_name] = df[geo_col].map(risk_mapping).fillna(2)  # Default medium risk

        self.logger.info(f"Created {len(self.config['feature_engineering']['derived_features'])} derived features")
        return derived_df

    def scale_features(self, df):
        if self.config['feature_engineering']['scaling']['method'] == 'none':
            self.logger.info("Skipping feature scaling")
            return df
            
        self.logger.info("Scaling numerical features...")
        
        scaled_df = df.copy()
        exclude_cols = self.config['feature_engineering']['scaling']['exclude_columns']
        
        # Identify numerical columns to scale
        numerical_cols = []
        for col in df.columns:
            if col not in exclude_cols and pd.api.types.is_numeric_dtype(df[col]):
                numerical_cols.append(col)
        
        if numerical_cols:
            scaler_method = self.config['feature_engineering']['scaling']['method']
            scaler = self.scalers[scaler_method]
            
            scaled_values = scaler.fit_transform(df[numerical_cols])
            scaled_df[numerical_cols] = scaled_values
            
            self.logger.info(f"Scaled {len(numerical_cols)} numerical features using {scaler_method} scaling")
        
        return scaled_df

    def setup_database(self):
        db_path = self.base_folder / self.config['database']['db_path']
        relative_db_path = os.path.relpath(str(db_path), str(self.base_folder))
        self.logger.info(f"Setting up database at: {relative_db_path}")
        
        # Create database connection
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create schema
        schema_commands = [
            """
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id INTEGER PRIMARY KEY,
                customer_num INTEGER UNIQUE,
                geography TEXT,
                gender TEXT,
                age REAL,
                age_group TEXT,
                tenure_days REAL,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_customer_features (
                feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER,
                balance_mean REAL,
                balance_std REAL,
                credit_score_mean REAL,
                estimated_salary_mean REAL,
                balance_to_salary_ratio REAL,
                credit_score_category TEXT,
                high_value_customer INTEGER,
                geographic_risk_score REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS transformation_metadata (
                transformation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transformation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                input_records INTEGER,
                output_records INTEGER,
                features_created INTEGER,
                transformation_config TEXT
            )
            """
        ]
        
        for command in schema_commands:
            cursor.execute(command)
        
        # Create indexes
        index_commands = [
            "CREATE INDEX IF NOT EXISTS idx_customers_geography ON dim_customers(geography)",
            "CREATE INDEX IF NOT EXISTS idx_customers_age_group ON dim_customers(age_group)",
            "CREATE INDEX IF NOT EXISTS idx_features_customer_id ON fact_customer_features(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_features_last_updated ON fact_customer_features(last_updated)"
        ]
        
        for command in index_commands:
            cursor.execute(command)
        
        conn.commit()
        self.logger.info("Database schema created successfully")
        return conn

    def load_to_database(self, df, conn):
        self.logger.info("Loading transformed data to database...")
        
        cursor = conn.cursor()
        
        # Insert into dim_customers
        customer_cols = ['CustomerId', 'Geography_mode', 'Gender_mode', 'Age_mean', 'age_group']
        customer_data = []
        
        for _, row in df.iterrows():
            customer_data.append((
                int(row['CustomerId']) if 'CustomerId' in row else int(row['CustomerNum']),
                row.get('Geography_mode', 'Unknown'),
                row.get('Gender_mode', 'Unknown'), 
                row.get('Age_mean', 0),
                row.get('age_group', 'Unknown')
            ))
        
        cursor.executemany("""
            INSERT OR REPLACE INTO dim_customers 
            (customer_id, geography, gender, age, age_group)
            VALUES (?, ?, ?, ?, ?)
        """, customer_data)
        
        # Insert into fact_customer_features
        feature_data = []
        for _, row in df.iterrows():
            feature_data.append((
                int(row['CustomerId']) if 'CustomerId' in row else int(row['CustomerNum']),
                row.get('Balance_mean', 0),
                row.get('Balance_std', 0),
                row.get('CreditScore_mean', 0),
                row.get('EstimatedSalary_mean', 0),
                row.get('balance_to_salary_ratio', 0),
                str(row.get('credit_score_category', 'Unknown')),
                int(row.get('high_value_customer', 0)),
                row.get('geographic_risk_score', 0)
            ))
        
        cursor.executemany("""
            INSERT OR REPLACE INTO fact_customer_features
            (customer_id, balance_mean, balance_std, credit_score_mean, 
             estimated_salary_mean, balance_to_salary_ratio, credit_score_category,
             high_value_customer, geographic_risk_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, feature_data)
        
        # Insert transformation metadata
        cursor.execute("""
            INSERT INTO transformation_metadata 
            (input_records, output_records, features_created, transformation_config)
            VALUES (?, ?, ?, ?)
        """, (len(df), len(df), len(df.columns), json.dumps(self.config)))
        
        conn.commit()
        self.logger.info(f"Loaded {len(df)} records to database")

    def generate_reports(self, df, input_shape):
        self.logger.info("Generating transformation reports...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save transformed dataset
        output_csv = self.output_dir / f"transformed_data_{timestamp}.csv"
        df.to_csv(output_csv, index=False)
        relative_output_csv = os.path.relpath(str(output_csv), str(self.base_folder))
        self.logger.info(f"Transformed data saved to: {relative_output_csv}")
        
        # Generate feature engineering summary
        summary = {
            'transformation_timestamp': datetime.now().isoformat(),
            'input_shape': input_shape,
            'output_shape': df.shape,
            'features_created': len(df.columns),
            'feature_list': df.columns.tolist(),
            'numerical_features': df.select_dtypes(include=[np.number]).columns.tolist(),
            'categorical_features': df.select_dtypes(exclude=[np.number]).columns.tolist(),
            'data_types': df.dtypes.astype(str).to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'statistical_summary': df.describe().to_dict()
        }
        
        summary_path = self.output_dir / f"feature_engineering_summary_{timestamp}.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=4)
        
        relative_summary_path = os.path.relpath(str(summary_path), str(self.base_folder))
        self.logger.info(f"Feature engineering summary saved to: {relative_summary_path}")
        
        return output_csv, summary_path

    def run(self):
        try:
            # Load input data
            df, metadata = self.load_data()
            input_shape = df.shape
            
            # Create aggregated features
            aggregated_df = self.create_aggregated_features(df)
            self.logger.info(f"Aggregated dataset shape: {aggregated_df.shape}")
            
            # Create derived features
            transformed_df = self.create_derived_features(aggregated_df)
            self.logger.info(f"Derived features dataset shape: {transformed_df.shape}")
            
            # Scale features
            scaled_df = self.scale_features(transformed_df)
            self.logger.info(f"Final transformed dataset shape: {scaled_df.shape}")
            
            # Setup database and load data
            conn = self.setup_database()
            self.load_to_database(scaled_df, conn)
            conn.close()
            
            # Generate reports
            output_csv, summary_path = self.generate_reports(scaled_df, input_shape)
            
            return output_csv, summary_path
            
        except Exception as e:
            self.logger.error(f"Data transformation failed: {e}")
            raise

if __name__ == "__main__":
    import pathlib
    
    script_folder = pathlib.Path(__file__).parent.resolve()
    logger = setup_logging(script_folder.parent)
    config = load_config(script_folder)
    transformer = DataTransformationManager(logger, script_folder, config)
    
    try:
        output_file, summary_file = transformer.run()
        print(f"Data transformation completed successfully!")
        base_folder = script_folder.parent.resolve()
        print(f"Transformed data saved at: {os.path.relpath(str(output_file), str(base_folder))}")
        print(f"Summary saved at: {os.path.relpath(str(summary_file), str(base_folder))}")
    except Exception as e:
        print(f"Data transformation failed: {e}")