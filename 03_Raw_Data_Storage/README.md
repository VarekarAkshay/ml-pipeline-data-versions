# Step 3: Raw Data Storage

This step focuses on storing the ingested raw data in a structured data lake using the local filesystem as the storage medium. The goal is to organize raw datasets efficiently to support easy retrieval, versioning, and pipeline traceability.

---

## Folder Structure (Relative to root directory: `03_Raw_Data_Storage`)

03_Raw_Data_Storage/
├── data_lake/
│ ├── kaggle/
│ │ └── raw/
│ │ └── YYYY-MM-DD/
│ │ └── <raw_data_files>.csv
│ ├── huggingface/
│ │ └── raw/
│ │ └── YYYY-MM-DD/
│ │ └── <raw_data_files>.csv
├── logs/
├── scripts/
│ └── store_raw_data.py
└── README.md

- **data_lake/** is the centralized root folder for raw data storage inside Step 3.
- **kaggle/** and **huggingface/** represent data sources.
- **raw/** specifies that these are raw (unprocessed) datasets.
- **YYYY-MM-DD/** partitions the data by ingestion date for better organization and traceability.
- **logs/** contains logs generated during data storage operations.
- **scripts/** contains scripts to automate the storage process.

---

## How to Execute Step 3: Raw Data Storage from Project Root Directory

1. **Open your terminal or command prompt.**

2. **Ensure you are in the project root directory.**  
   This is the top-level folder containing all step folders like `02_Data_Ingestion` and `03_Raw_Data_Storage`.

3. **Run the raw data storage script with the following command from root directory:**
    python ./03_Raw_Data_Storage/scripts/upload_raw_data.py


4. **What happens during execution:**

- The script reads raw data files generated in Step 2 located at:
  - `02_Data_Ingestion/raw_data/kaggle/`
  - `02_Data_Ingestion/raw_data/huggingface/`
  
- It copies these raw files into the data lake folders partitioned by source, data type (`raw`), and ingestion date inside:
  - `03_Raw_Data_Storage/data_lake/`

5. **Logs are generated at:**  
`03_Raw_Data_Storage/logs/` with detailed information on copied files, timestamps, and any errors encountered.

---

## Prerequisites

- Ensure Step 2 ingestion step has successfully completed with raw data files available.
- Python 3.8+ installed and accessible in your environment.
- Required dependencies installed (e.g., via `pip install -r ./02_Data_Ingestion/scripts/requirements.txt`).

---

This execution method maintains consistency by referencing all paths relative to the project root, allowing seamless integration into the overall pipeline orchestration.



## Design Considerations

- Using the local file system under the `03_Raw_Data_Storage` directory simulates a data lake environment.
- Partitioning by source, data type, and timestamp allows scalable and auditable storage.
- The structure supports future expansion to other sources and data types.

---

## Deliverables

- **Folder Structure Documentation:** This README describing the directory hierarchy and organization of raw data.
- **Python Script:** Automates moving or copying raw ingested files from Step 2 locations into the data lake partitioned folders under `03_Raw_Data_Storage/data_lake`.
- **Logging:** All file operations are logged with timestamps and statuses in the `logs/` folder.
- **Execution:** Scripts reference paths relative to the project root to allow flexible pipeline orchestration.

---

This approach ensures that ingested raw datasets are archived in an organized, modular, and timestamped manner inside the Step 3 folder, building a robust foundation for downstream processes.







