import os
import sys
import time
import logging
import zipfile
import json
from datetime import datetime
import yaml
import shutil
import pathlib


current_dir = pathlib.Path(__file__).parent.resolve()
project_root = current_dir.parents[1]


step2_root = pathlib.Path(__file__).parent.parent.resolve()  # 02_Data_Ingestion folder
cfg_path = step2_root / 'config.yaml'
cfg = yaml.safe_load(open(cfg_path))



k = cfg['kaggle']



cred_path = step2_root / k.get('credentials_path')
if not cred_path.is_file():
    print(f"ERROR: Kaggle JSON not found at {cred_path}")
    sys.exit(1)
os.environ['KAGGLE_CONFIG_DIR'] = str(cred_path.parent)



log_dir = step2_root / k['log_dir']
os.makedirs(log_dir, exist_ok=True)



def extract_string(item):
    while isinstance(item, list) and len(item) > 0:
        item = item[0]
    return item



def ingest():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_file = log_dir / f"ingest_kaggle_{datetime.now():%Y%m%d}.log"
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger()


    print(f"\n==== Kaggle Ingestion starting at {timestamp} ====")
    logger.info(f"=== Ingestion starting at {timestamp} ===")


    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()


        raw_dir = step2_root / k['raw_data_dir']
        clean_dir = step2_root / k['clean_data_dir']
        clean_csv_path = step2_root / k['clean_output_csv']


        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(clean_dir, exist_ok=True)


        relative_raw_dir = os.path.relpath(str(raw_dir), str(project_root))
        logger.info(f"Downloading dataset {k['dataset']} to {relative_raw_dir}")
        api.dataset_download_files(k['dataset'], path=str(raw_dir), unzip=False)


        zip_files = [f for f in os.listdir(raw_dir) if f.endswith('.zip')]
        logger.info(f"Zip files found: {zip_files}")
        if not zip_files:
            raise FileNotFoundError("No zip file found in raw_data_dir")


        zip_filename = extract_string(zip_files[0])
        logger.info(f"Zip filename after extraction: {zip_filename}")
        if not isinstance(zip_filename, str):
            raise TypeError(f"Expected string filename but got {type(zip_filename)}")


        zip_path = raw_dir / zip_filename
        with zipfile.ZipFile(str(zip_path), 'r') as z:
            z.extractall(str(raw_dir))
        os.remove(str(zip_path))


        csv_files = [f for f in os.listdir(raw_dir) if f.lower().endswith('.csv')]
        logger.info(f"CSV files found: {csv_files}")
        if not csv_files:
            raise FileNotFoundError("No CSV file found after extraction")
        raw_csv_path = raw_dir / csv_files[0]


        shutil.copy(str(raw_csv_path), str(clean_csv_path))


        relative_log_path = os.path.relpath(str(log_file), str(project_root))
        relative_raw_path = os.path.relpath(str(raw_csv_path), str(project_root))
        relative_clean_path = os.path.relpath(str(clean_csv_path), str(project_root))

        success_msg = (f"Kaggle ingestion successful!\n"
                       f"Raw data: {relative_raw_path}\n"
                       f"Cleaned data: {relative_clean_path}")

        print(f"PASSED: {success_msg}")
        print(f"See logs at relative path: {relative_log_path}\n")
        logger.info(success_msg)
        
    except Exception as e:
        relative_log_path = os.path.relpath(str(log_file), str(project_root))
        fail_msg = f"Kaggle ingestion failed: {e}\nCheck logs: {relative_log_path}"
        print(f"FAILED: {fail_msg}")
        logger.error(fail_msg)
        raise


if __name__ == '__main__':
    for attempt in range(1, cfg['retry_attempts'] + 1):
        try:
            ingest()
            break
        except Exception:
            if attempt < cfg['retry_attempts']:
                time.sleep(5)
            else:
                print("FAILED: All Kaggle ingestion attempts have failed. Exiting.")
                sys.exit(1)
