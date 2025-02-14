# File Standardization ETL Job

This AWS Glue ETL job standardizes file formats from various sources into a consistent format for data processing.

## Overview

The ETL job processes files from different sources, applying standardization rules based on entity and attribute configurations stored in a database. It supports both stock and sales data processing with different validation and transformation rules.

## Architecture

- **Main Components**:
  - File Processing
  - Entity Matching
  - Data Transformation
  - Data Loading

- **Key Features**:
  - Sequential entity matching strategy
  - Configurable attribute mapping
  - Support for multiple file formats
  - Comprehensive error handling and logging
  - Special case handling for different data owners

## Project Structure

```
.
├── config/
│   ├── queries.py        # SQL query templates
│   ├── settings.py       # Configuration settings
│   └── db_config.py      # Database configuration
├── src/
│   ├── database/
│   │   ├── connection.py    # Database connection handling
│   │   └── operations.py    # Database operations
│   ├── etl/
│   │   ├── extractors.py    # Data extraction
│   │   ├── transformers.py  # Data transformation
│   │   └── loaders.py       # Data loading
│   ├── business/
│   │   ├── rules.py         # Business rules
│   │   └── special_cases.py # Special case handling
│   ├── validation/
│   │   ├── schema_validator.py  # Schema validation
│   │   └── data_validator.py    # Data validation
│   └── utils/
│       ├── logging_utils.py     # Logging utilities
│       ├── s3_utils.py          # S3 operations
│       └── date_utils.py        # Date handling utilities
└── file_standardization_job.py  # Main job script
```

## Setup

1. **AWS Environment Requirements**:
   - AWS Glue service role with appropriate permissions
   - S3 bucket for job scripts and data
   - Secrets Manager for database credentials

2. **Database Requirements**:
   - PostgreSQL database
   - Required tables:
     - `daq_log_info`
     - `dd_entity_detail`
     - `dd_attribute_detail`
     - `temp_load_info`

3. **Configuration**:
   - Update `config/settings.py` with environment-specific settings
   - Configure database connection in AWS Secrets Manager
   - Set up appropriate IAM roles and permissions

## Deployment

1. Package the code:
   ```bash
   # Create deployment package
   mkdir deployment
   cp -r src config file_standardization_job.py deployment/
   ```

2. Upload to S3:
   ```bash
   aws s3 cp deployment s3://your-bucket/glue-jobs/file-standardization/ --recursive
   ```

3. Create AWS Glue job:
   - Script path: `s3://your-bucket/glue-jobs/file-standardization/file_standardization_job.py`
   - Worker type: G.1X or G.2X
   - Python version: 3.9
   - Job parameters:
     - `--JOB_NAME`
     - `--secret_name`
     - `--env`

## Monitoring

- CloudWatch Logs for detailed job execution logs
- Glue Job metrics for performance monitoring
- Database logs for data processing validation

## Error Handling

The job includes comprehensive error handling:
- File-level error handling
- Entity-level error handling
- Data validation errors
- Database operation errors
- AWS service errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
