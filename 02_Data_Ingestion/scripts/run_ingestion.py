import os
import sys
import subprocess
import logging
from datetime import datetime
import yaml
import pathlib

current_dir = pathlib.Path(__file__).parent.resolve()
step2_root = current_dir.parent  # 02_Data_Ingestion folder

cfg_path = step2_root / 'config.yaml'
cfg = yaml.safe_load(open(cfg_path))

log_dir = step2_root / 'logs'
os.makedirs(log_dir, exist_ok=True)

log_file = log_dir / f"run_ingestion_{datetime.now():%Y%m%d}.log"
logging.basicConfig(
    filename=str(log_file),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger()


def run_script(script_name, script_description):
    script_path = str(step2_root / "scripts" / script_name)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Starting {script_description} at {timestamp}")
    print(f"\n==== Starting {script_description} at {timestamp} ====")

    try:
        proc = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=300
        )

        if proc.returncode == 0:
            logger.info(f"{script_description} completed successfully")
            print(f"PASSED: {script_description} completed successfully")
            if proc.stdout.strip():
                logger.info(f"{script_description} output:\n{proc.stdout.strip()}")
        else:
            logger.error(f"{script_description} failed with return code {proc.returncode}")
            logger.error(f"{script_description} stderr:\n{proc.stderr.strip()}")
            relative_log_path = os.path.relpath(str(log_file), str(step2_root))
            print(f"FAILED: {script_description} failed. See log: {relative_log_path}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"{script_description} timed out")
        relative_log_path = os.path.relpath(str(log_file), str(step2_root))
        print(f"FAILED: {script_description} timed out. See log: {relative_log_path}")
        return False
    except Exception as e:
        logger.error(f"{script_description} failed with exception: {e}")
        relative_log_path = os.path.relpath(str(log_file), str(step2_root))
        print(f"FAILED: {script_description} failed with exception: {e}. See log: {relative_log_path}")
        return False

    return True


def main():
    logger.info("Starting master ingestion run")
    print("=" * 50)
    print("Data Ingestion Master Script")
    print("=" * 50)

    scripts = [
        ("ingest_kaggle.py", "Kaggle Data Ingestion"),
        ("ingest_hf.py", "Hugging Face Data Ingestion"),
    ]

    successes = 0

    for script_file, desc in scripts:
        if run_script(script_file, desc):
            successes += 1
        print("-" * 30)

    print("=" * 50)
    print(f"Ingestion Summary: {successes}/{len(scripts)} scripts completed successfully")

    if successes == len(scripts):
        logger.info("All ingestion scripts completed successfully")
        print("PASSED: All ingestion processes completed successfully!")
        return 0
    else:
        logger.error("Some ingestion scripts failed")
        print("FAILED: Some ingestion processes failed. Check logs for details.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
