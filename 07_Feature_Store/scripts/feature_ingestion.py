import os
import sqlite3
import logging
from datetime import datetime
import yaml
import pathlib
import pandas as pd
import json

from feature_store_manager import RelativePathFilter

def setup_logging(base_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = base_path / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"feature_ingestion_{timestamp}.log"

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(relativepathname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger('feature_ingestion')
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
        raise FileNotFoundError(f"Config not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)

def ingest_features():
    script_folder = pathlib.Path(__file__).parent.resolve()
    base_folder = script_folder.parent.resolve()
    logger = setup_logging(base_folder)
    config = load_config(script_folder)

    # Connect to Step 6 data warehouse
    step6_db = base_folder / config['input_sources']['step6_datawarehouse']
    if not step6_db.exists():
        logger.error(f"Step 6 datawarehouse not found: {step6_db}")
        return

    # Connect to feature store
    fs_db = base_folder / config['feature_store']['database_path']
    fs_db.parent.mkdir(parents=True, exist_ok=True)

    step6_conn = sqlite3.connect(step6_db)
    fs_conn = sqlite3.connect(fs_db)

    try:
        logger.info(f"Ingesting features from {os.path.relpath(str(step6_db), str(base_folder))}")

        # Example ingestion query (should match your feature mappings)
        df = pd.read_sql_query("""
            SELECT customer_id, balance_mean, credit_score_mean, high_value_customer
            FROM fact_customer_features
        """, step6_conn)

        ingested = 0
        now = datetime.now().isoformat()
        for _, row in df.iterrows():
            entity = str(row['customer_id'])
            features = {
                'balance_mean': row['balance_mean'],
                'credit_score_mean': row['credit_score_mean'],
                'high_value_customer': int(row['high_value_customer'])
            }
            for fname, val in features.items():
                fid = f"{fname}_v{config['features']['customer_financial']['features'][0]['version']}"
                fs_conn.execute("""
                    INSERT OR REPLACE INTO online_features
                    (entity_id, feature_id, feature_value, last_updated)
                    VALUES (?, ?, ?, ?)
                """, (entity, fid, json.dumps(val), now))
                fs_conn.execute("""
                    INSERT OR REPLACE INTO offline_features
                    (entity_id, feature_id, feature_value, timestamp)
                    VALUES (?, ?, ?, ?)
                """, (entity, fid, json.dumps(val), now))

                ingested += 1

        fs_conn.commit()
        logger.info(f"Ingested {ingested} feature values into Feature Store")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
    finally:
        step6_conn.close()
        fs_conn.close()

if __name__ == "__main__":
    ingest_features()

