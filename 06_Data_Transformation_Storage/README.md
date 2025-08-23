# Step 6: Data Transformation & Storage Pipeline

## Overview
This pipeline stage performs advanced feature engineering on the cleaned dataset from Step 5, creates aggregated features, derives new meaningful features, and stores the transformed data in a SQLite data warehouse with proper schema design.

## Features
- **Advanced Feature Engineering**: Creates aggregated features (total spend per customer, transaction frequency)
- **Derived Features**: Customer tenure, activity frequency, customer lifecycle metrics
- **SQLite Data Warehouse**: Implements a structured data warehouse with proper schema
- **Feature Scaling**: Normalizes and scales features where necessary
- **Schema Documentation**: Complete SQL schema with relationships and constraints
- **Sample Queries**: Pre-built queries for common data retrieval patterns
- **Transformation Logic**: Detailed documentation of all transformation steps

## Architecture
```
06_Data_Transformation_Storage/
├── config_step6.yaml          # Configuration file linking Step 5 outputs
├── scripts/
│   ├── data_transformation.py  # Main transformation pipeline
│   └── schema_setup.py         # Database schema creation
├── sql/
│   ├── schema.sql              # Complete database schema
│   └── sample_queries.sql      # Example queries for data retrieval
├── reports/                    # Output directory for transformed data
├── database/                   # SQLite database storage
├── logs/                       # Pipeline execution logs
└── README.md                   # This file
```

## Prerequisites

### System Requirements
- Python 3.8+
- SQLite 3.31+
- Minimum 4GB RAM
- 2GB free disk space

### Python Dependencies
```bash
pip install pandas numpy sqlite3 pyyaml scikit-learn sqlalchemy logging pathlib
```

### Input Requirements
- **Step 5 Outputs**: Cleaned dataset CSV from `05_Data_Preparation/reports/`
- **Step 5 Metadata**: Processing metadata JSON from Step 5
- **Configuration**: Valid `config_step6.yaml` with correct paths

### Data Requirements
- Clean dataset with no missing values in key columns
- Standardized numerical features from Step 5
- Encoded categorical variables from Step 5
- Valid customer identifiers and timestamps

## Configuration
config_step6.yaml 

## Execution

### Command Line Execution
```bash
# Execute from project root directory
python ./06_Data_Transformation_Storage/scripts/data_transformation.py


### Execution Steps
1. **Configuration Loading**: Loads and validates configuration
2. **Input Validation**: Verifies Step 5 outputs exist and are valid
3. **Feature Engineering**: Creates aggregated and derived features
4. **Database Setup**: Creates SQLite schema and tables
5. **Data Loading**: Loads transformed data into database
6. **Quality Validation**: Validates transformed data integrity
7. **Report Generation**: Creates transformation summary report

## Feature Engineering Details

### Aggregated Features
- **Total Spend per Customer**: Sum of all transaction amounts
- **Transaction Count**: Number of transactions per customer
- **Average Transaction Amount**: Mean transaction value per customer
- **Activity Frequency**: Transactions per time period
- **Geographic Concentration**: Primary geography usage patterns

### Derived Features
- **Customer Tenure**: Days since account opening
- **Spending Velocity**: Rate of spending change over time
- **Balance Trend**: Direction of account balance changes
- **Product Affinity**: Preference patterns for different products
- **Risk Indicators**: Derived risk metrics from behavioral patterns

### Temporal Features
- **Seasonal Patterns**: Monthly/quarterly activity patterns
- **Day-of-Week Effects**: Weekday vs weekend behavior
- **Time-since-Last-Activity**: Days since last transaction
- **Activity Consistency**: Regularity of account usage

## Output Files

### Generated Outputs
- **Transformed Dataset**: `reports/transformed_data_YYYYMMDD_HHMMSS.csv`
- **Feature Summary**: `reports/feature_engineering_summary_YYYYMMDD_HHMMSS.json`
- **SQLite Database**: `database/customer_data_warehouse.db`
- **Schema Documentation**: Auto-generated schema documentation
- **Transformation Report**: Detailed transformation log and statistics

### Quality Reports
- **Data Quality Metrics**: Completeness, consistency, validity checks
- **Feature Distribution Analysis**: Statistical summaries of new features
- **Correlation Analysis**: Feature correlation matrices
- **Transformation Validation**: Before/after data comparison

## Monitoring & Logging

### Log Files
- **Transformation Logs**: `logs/data_transformation_YYYYMMDD_HHMMSS.log`
- **Database Logs**: `logs/database_operations_YYYYMMDD_HHMMSS.log`
- **Error Logs**: `logs/errors_YYYYMMDD_HHMMSS.log`

### Monitoring Metrics
- Processing time per transformation step
- Memory usage during feature creation
- Database write performance
- Data quality scores
- Feature engineering success rates

## Error Handling

### Common Issues & Solutions
1. **Missing Step 5 Output**: Verify Step 5 completed successfully
2. **Database Lock**: Ensure no other processes accessing SQLite database
3. **Memory Issues**: Reduce batch size in configuration
4. **Feature Creation Errors**: Check input data quality and types
5. **Schema Conflicts**: Drop and recreate database if schema changes

### Troubleshooting
```bash
# Check Step 5 outputs exist
ls -la ../05_Data_Preparation/reports/

# Verify database accessibility
sqlite3 database/customer_data_warehouse.db ".tables"

# Check log files for detailed errors
tail -f logs/data_transformation_*.log
```

## Performance Optimization

### Batch Processing
- Configurable batch sizes for large datasets
- Memory-efficient feature computation
- Incremental processing for updates

### Database Optimization
- Proper indexing on key columns
- Query optimization for common patterns
- Connection pooling for concurrent access

### Scalability Considerations
- Horizontal scaling through data partitioning
- Parallel processing for independent transformations
- Caching frequently accessed features

## Integration with Pipeline

### Upstream Dependencies
- **Step 5**: Clean dataset and preprocessing metadata
- **Configuration**: Valid YAML configuration file
- **System Resources**: Adequate disk space and memory

### Downstream Integration
- **Step 7**: Feature selection and model preparation
- **Analytics Tools**: Direct database access for BI tools
- **API Services**: RESTful access to transformed features

## Validation & Testing

### Data Validation Checks
- Input data completeness and format validation
- Feature creation mathematical correctness
- Database integrity and constraint validation
- Output data quality and consistency checks

### Testing Framework
```bash
# Run validation tests
python scripts/validate_transformation.py

# Check database integrity
python scripts/database_validation.py

# Verify feature correctness
python scripts/feature_validation.py
```

## Maintenance

### Regular Tasks
- Database backup and maintenance
- Log file rotation and cleanup
- Performance monitoring and optimization
- Schema updates and migrations

### Update Procedures
1. Backup existing database
2. Update configuration files
3. Run transformation with new parameters
4. Validate results and rollback if necessary

## Support & Documentation

For detailed technical documentation, see:
- `sql/schema.sql` - Complete database schema
- `sql/sample_queries.sql` - Query examples
- `reports/transformation_logic.md` - Detailed transformation documentation
- Pipeline logs for execution details

For issues and support, check the troubleshooting section and review log files for detailed error messages.