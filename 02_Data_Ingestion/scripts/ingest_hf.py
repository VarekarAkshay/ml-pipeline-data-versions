import os
import sys
import time
import logging
from datetime import datetime
import pandas as pd
import requests
from io import StringIO
import yaml
import pathlib

# Set Step 2 root directory (02_Data_Ingestion folder)
step2_root = pathlib.Path(__file__).parent.parent.resolve()
# Set project root directory (one level above step2_root)
project_root = step2_root.parent

cfg_path = step2_root / 'config.yaml'
cfg = yaml.safe_load(open(cfg_path))

h = cfg['huggingface']

log_dir = step2_root / h['log_dir']
os.makedirs(log_dir, exist_ok=True)

def ingest():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_file = log_dir / f"ingest_hf_{datetime.now():%Y%m%d}.log"
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger()

    print(f"\n==== Hugging Face Ingestion starting at {timestamp} ====")
    logger.info(f"=== Ingestion starting at {timestamp} ===")

    try:
        response = requests.get(h['url'])
        response.raise_for_status()

        raw_dir = step2_root / h['raw_data_dir']
        clean_dir = step2_root / h['clean_data_dir']
        clean_csv_path = step2_root / h['clean_output_csv']

        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(clean_dir, exist_ok=True)

        raw_csv_path = raw_dir / "raw_bank_customer_churn.csv"
        with open(raw_csv_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        relative_raw_path = os.path.relpath(str(raw_csv_path), str(project_root))
        logger.info(f"Raw data saved to {relative_raw_path}")

        df = pd.read_csv(StringIO(response.text))
        expected_columns = [
            "CustomerId", "Surname", "CreditScore", "Geography", "Gender",
            "Age", "Tenure", "Balance", "NumOfProducts", "HasCrCard",
            "IsActiveMember", "EstimatedSalary", "Exited"
        ]
        df = df.reindex(columns=expected_columns)
        for col in expected_columns:
            if col not in df.columns:
                df[col] = pd.NA

        df.to_csv(clean_csv_path, index=False)

        relative_clean_path = os.path.relpath(str(clean_csv_path), str(project_root))
        logger.info(f"Cleaned data saved to {relative_clean_path}")

        relative_log_path = os.path.relpath(str(log_file), str(project_root))
        success_msg = (f"Hugging Face ingestion successful!\n"
                       f"Raw data: {relative_raw_path}\n"
                       f"Cleaned data: {relative_clean_path}")

        print(f"PASSED: {success_msg}")
        print(f"See logs at relative path: {relative_log_path}\n")
        logger.info(success_msg)

    except Exception as e:
        relative_log_path = os.path.relpath(str(log_file), str(project_root))
        fail_msg = f"Hugging Face ingestion failed: {e}\nCheck logs: {relative_log_path}"
        print(f"FAILED: {fail_msg}")
        logger.error(fail_msg)
        raise

if __name__ == "__main__":
    for attempt in range(1, cfg['retry_attempts'] + 1):
        try:
            ingest()
            break
        except Exception:
            if attempt < cfg['retry_attempts']:
                time.sleep(5)
            else:
                print("FAILED: All Hugging Face ingestion attempts have failed. Exiting.")
                sys.exit(1)
