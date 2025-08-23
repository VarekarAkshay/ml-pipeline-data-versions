# Step 8: Data Versioning

## Overview
This step implements comprehensive data versioning across the entire ML pipeline, ensuring reproducibility, traceability, and auditability of raw, transformed, and feature-engineered datasets.

It integrates Git for code, config, and metadata version control, and DVC for large dataset management with optional remote cloud storage.

## Features
- Full version control of datasets and metadata from Steps 1 to 7
- Semantic versioning based on schema and data changes
- Automated change detection and tagging
- Metadata including file hashes, timestamps, lineage, and quality metrics
- Integration with GitHub for collaboration and pipeline automation
- Reports including version summaries, data lineage, and changelogs
- Validation tools for repository and data integrity

## Folder Structure

08_Data_Versioning/
├── config_step8.yaml # Configuration including all prior steps and versioning parameters
├── README.md # This documentation file
├── requirements_step8.txt # Python packages required
├── scripts/
│ ├── data_versioning_manager.py # Main versioning manager
│ ├── version_validator.py # Validation and integrity checks
│ ├── dvc_manager.py # DVC operations (optional modular)
│ ├── git_manager.py # Git operations (optional modular)
│ └── metadata_manager.py # Metadata and lineage utilities (optional modular)
├── dvc/ # DVC repository and cache
├── git/ # Git repo and history
├── metadata/ # Stored version metadata and lineage
├── reports/ # Generated version reports and changelogs
├── logs/ # Execution logs
└── backups/ # Backup snapshots


## Prerequisites & Installation

- Python 3.5+ (tested with 3.6–3.10)
- Git 2.20+
- DVC 3.5+
- Optional: AWS/GCP/Azure remote for DVC cache

### Python Dependencies

Install dependencies via:

pip install -r requirements.txt


## Usage

Run data versioning from project root:

python ./08_Data_Versioning/scripts/data_versioning_manager.py


Options:

- `--init-all` : Initialize baseline version with all pipeline data.
- `--step <n>` : Version only specific step (e.g., `6` for transformation data).
- `--tag <tag>` : Create version with specific tag.
- `--description <desc>` : Provide description for the version.
- `--check-all` (with `version_validator.py`) : Run all validation checks.

Examples:


python ./08_Data_Versioning/scripts/data_versioning_manager.py --init-all --description "Initial pipeline baseline version"
python ./08_Data_Versioning/scripts/data_versioning_manager.py --step 7 --description "Feature store update"
python ./08_Data_Versioning/scripts/version_validator.py --check-all --verbose


## Outputs

- Git repository under `git/` with commits and tags
- DVC repository under `dvc/` with tracked data files
- Version metadata JSON files under `metadata/versions/`
- Version summary, lineage, and changelog reports under `reports/`
- Logs describing executed actions under `logs/`

## Workflow Integration

Automatically triggered by pipeline step completions or manually for audit purposes.

Chain with continuous integration via GitHub Actions workflows included (`.github/workflows/`) for validation and release management.

---

## Execution Instructions

### 1. Initialize Git & DVC Repositories

From the `08_Data_Versioning` folder or project root:

Initialize Git repository if not already created
git init git/
cd git/
git remote add origin https://github.com/your-username/ml-pipeline-data-versions.git
cd ../

Initialize DVC repository
dvc init --subdir dvc/
cd dvc/

(Optional) Add remote storage if using cloud (update URL accordingly)
dvc remote add -d origin s3://your-bucket/data-versions
cd ../


---

### 2. Install Python Dependencies

Activate your virtual environment and install dependencies:

pip install --upgrade pip
pip install -r requirements.txt


---

### 3. Create Initial Data Version

Create the baseline version capturing all pipeline data:

python ./08_Data_Versioning/scripts/data_versioning_manager.py --init-all --description "Initial pipeline baseline version"


Check logs under `08_Data_Versioning/logs/`.

---

### 4. Create Incremental or Step-Specific Versions

For specific step data (example: Step 6):

python ./08_Data_Versioning/scripts/data_versioning_manager.py --step 6 --description "Updated data transformation results"


Or create a tagged version:

python ./08_Data_Versioning/scripts/data_versioning_manager.py --tag v1.2.0 --description "Feature store enhancement and data refresh"


---

### 5. Validate Version Metadata and Integrity

Run validation to ensure repository health and data integrity:

python ./08_Data_Versioning/scripts/version_validator.py --check-all --verbose


Review report in `08_Data_Versioning/reports/`.

---

### 6. Switch Between Data Versions

Checkout specific tag and sync datasets:

git checkout v1.0.0
dvc checkout


Restores all data and metadata to the specified version.

---

### 7. Automate with CI/CD

Leverage GitHub Actions workflows to automate validation and versioning on code or data changes (templates provided).

---

### 8. Monitor and Audit

- Review execution logs: `08_Data_Versioning/logs/`
- Examine version metadata: `08_Data_Versioning/metadata/versions/`
- Use reports for compliance and progress tracking: `08_Data_Versioning/reports/`

---


This README provides a full overview and practical instructions for executing Step 8 Data Versioning in your ML pipeline. If you want, I can provide the updated config and full scripts next.


