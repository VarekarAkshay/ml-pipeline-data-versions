import pathlib
import yaml
import os
import sys
import shutil
import logging
from datetime import datetime


# Current script directory (03_Raw_Data_Storage/scripts)
current_dir = pathlib.Path(__file__).parent.resolve()

# Step 3 root folder (03_Raw_Data_Storage)
step3_root = current_dir.parent

# Load Step 3 config relative to Step 3 folder
step3_cfg_path = step3_root / 'config_step3.yaml'
step3_cfg = yaml.safe_load(open(step3_cfg_path))

# Resolve Step 2 config path relative to Step 3 folder (as per config)
step2_cfg_path = step3_root / step3_cfg['step2_config_path']
step2_cfg = yaml.safe_load(open(step2_cfg_path))

# Determine Step 2 root folder (parent directory of Step 2 config)
step2_root = step2_cfg_path.parent

# Setup datalake and logging folders inside Step 3 folder
datalake_root = step3_root / step3_cfg['raw_data_storage']['datalake_root']
log_dir = step3_root / step3_cfg['raw_data_storage']['log_dir']
log_dir.mkdir(parents=True, exist_ok=True)

# Configure logger
log_file = log_dir / f"upload_raw_{datetime.now():%Y%m%d}.log"
logging.basicConfig(filename=str(log_file), level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger()

# Define project root for relative paths (parent of Step folders)
project_root = step3_root.parent

# Resolve raw data directories relative to Step 2 root
sources = {
    'kaggle': step2_root / step2_cfg['kaggle']['raw_data_dir'],
    'huggingface': step2_root / step2_cfg['huggingface']['raw_data_dir']
}

def check_raw_data_exists(source_name, raw_dir):
    rel_raw_dir = os.path.relpath(raw_dir, start=project_root)
    if not raw_dir.exists():
        print(f"FAILED: Raw data directory for {source_name} does not exist: {rel_raw_dir}")
        logger.error(f"Raw data directory for {source_name} does not exist: {rel_raw_dir}")
        return False

    if not any(f.is_file() for f in raw_dir.iterdir()):
        print(f"FAILED: Raw data directory for {source_name} is empty: {rel_raw_dir}")
        logger.error(f"Raw data directory for {source_name} is empty: {rel_raw_dir}")
        return False

    return True

def copy_to_datalake(source_name, raw_dir):
    rel_raw_dir = os.path.relpath(raw_dir, start=project_root)
    print(f"\n==== Uploading {source_name} raw data to data lake ====")
    logger.info(f"Uploading {source_name} raw data from {rel_raw_dir}")

    if not check_raw_data_exists(source_name, raw_dir):
        return False

    date_part = datetime.now().strftime("%Y-%m-%d")
    dest_dir = datalake_root / source_name / 'raw' / date_part
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        for file_path in raw_dir.iterdir():
            if file_path.is_file():
                dest_file = dest_dir / file_path.name
                shutil.copy2(str(file_path), str(dest_file))
                rel_src = os.path.relpath(file_path, start=project_root)
                rel_dst = os.path.relpath(dest_file, start=project_root)
                logger.info(f"Copied {rel_src} to {rel_dst}")
        rel_dest_dir = os.path.relpath(dest_dir, start=project_root)
        print(f"PASSED: {source_name} raw data uploaded to {rel_dest_dir}")
        logger.info(f"{source_name} raw data upload successful")
        rel_log = os.path.relpath(log_file, start=project_root)
        print(f"See logs at relative path: {rel_log} \n")
        return True
    except Exception as e:
        print(f"FAILED: Error uploading {source_name} raw data: {e}")
        logger.error(f"Error uploading {source_name} raw data: {e}")
        rel_log = os.path.relpath(log_file, start=project_root)
        print(f"See logs at relative path: {rel_log} \n")
        return False

def main():
    print("="*50)
    print("Step 3: Raw Data Storage - Upload to Data Lake")
    print("="*50)
    logger.info("===== Step 3 Raw Data Storage started =====")

    all_success = True
    missing_data_sources = []

    for source_name, raw_dir in sources.items():
        if not check_raw_data_exists(source_name, raw_dir):
            missing_data_sources.append(source_name)
            all_success = False

    if missing_data_sources:
        print(f"\nERROR: Raw data missing for source(s): {', '.join(missing_data_sources)}")
        print("Please run Step 2 ingestion scripts to generate raw data before running Step 3.")
        logger.error(f"Raw data missing for: {missing_data_sources}. Step 2 must be run first.")
        sys.exit(1)

    for source_name, raw_dir in sources.items():
        if not copy_to_datalake(source_name, raw_dir):
            all_success = False

    if all_success:
        print("PASSED: All raw data uploaded successfully.")
        logger.info("All raw data uploaded successfully.")
        return 0
    else:
        print("FAILED: Some uploads failed. Check logs.")
        logger.error("Some uploads failed.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
