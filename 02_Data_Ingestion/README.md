# Step 2: Data Ingestion

This step performs ingestion of raw datasets from two sources:

- **Kaggle**: Downloads and extracts the bank customer churn dataset using the Kaggle API.
- **Hugging Face**: Downloads the dataset CSV from the Hugging Face hub.

The raw data is saved locally, cleaned datasets are produced, and detailed logs are maintained with automatic retry attempts on failure.

---

## Folder Structure (relative to project root)

02_Data_Ingestion/
├── raw_data/
│   ├── kaggle/
│   └── huggingface/
├── clean_data/
│   ├── kaggle/
│   └── huggingface/
├── logs/
├── scripts/
│   ├── ingest_kaggle.py
│   ├── ingest_hf.py
│   └── run_ingestion.py
└── requirements.txt


- **raw_data/**: Stores raw files directly ingested from sources.
- **clean_data/**: Stores cleaned CSV outputs ready for further processing.
- **logs/**: Contains logs for all ingestion attempts with timestamps.
- **scripts/**: Contains individual ingestion scripts and orchestrator.
- **requirements.txt**: Python dependencies to run scripts.

---

## Prerequisites

- Python 3.8 or higher installed.
- Internet connectivity.
- Kaggle API token (`kaggle.json`) placed according to the path specified in `config.yaml`.
- All required Python packages installed using:

pip install -r ./02_Data_Ingestion/scripts/requirements.txt


---

## Configuration

- The shared `config.yaml` file in the project root directory drives all path and dataset settings.
- Paths in configs are relative to the **project root directory** to ensure consistency.


---

## Run the raw data Ingestion script with the following command from root directory:

You can run ingestion scripts individually or orchestrated via the main script.

- Run Kaggle ingestion:

python ./02_Data_Ingestion/scripts/ingest_kaggle.py


- Run Hugging Face ingestion:

python ./02_Data_Ingestion/scripts/ingest_hf.py


- Run both sequentially with orchestrator (recommended):

python ./02_Data_Ingestion/scripts/run_ingestion.py


---

## Output Locations

After successful runs you will find:

- Raw files ingested from:
  - `02_Data_Ingestion/raw_data/kaggle/`
  - `02_Data_Ingestion/raw_data/huggingface/`

- Cleaned CSV files at:
  - `02_Data_Ingestion/clean_data/kaggle/customer_churn.csv`
  - `02_Data_Ingestion/clean_data/huggingface/hf_customer_churn_cleaned.csv`

- Logs in:
  - `02_Data_Ingestion/logs/` with files like `ingest_kaggle_YYYYMMDD.log` and `ingest_hf_YYYYMMDD.log`

---

## Error Handling & Retries

- Scripts include automatic retry attempts configured in `config.yaml` (`retry_attempts`).
- On failure, detailed error messages are logged and presented in console.
- Ensure your Kaggle credentials are correctly set up to avoid authentication failures.

---

## Troubleshooting

- Missing or empty raw data folders indicate ingestion failure — check logs.
- For Kaggle ingestion authentication issues, verify the presence and correctness of your Kaggle API credentials file.
- For network errors downloading the Hugging Face dataset, confirm internet connection.

---

## Support

For assistance, please contact the data engineering or project support team.

---

