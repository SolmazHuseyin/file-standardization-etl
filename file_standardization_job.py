"""
Main ETL job script for file standardization.
"""

import sys
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

# Add deployment package to Python path
if 'GLUE_DEPLOYMENT_PATH' in os.environ:
    deployment_path = os.environ['GLUE_DEPLOYMENT_PATH']
else:
    # Default to the script's directory
    deployment_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(deployment_path)

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, SparkSession

from src.database.connection import DatabaseConnection
from src.database.operations import DatabaseOperations
from src.etl.extractors import DataExtractor
from src.etl.transformers import DataTransformer
from src.etl.loaders import DataLoader
from src.utils.logging_utils import ETLLogger
from src.utils.metrics import MetricsCollector

@dataclass
class FileInfo:
    """Data class for file information."""
    file_path: str
    file_type: str
    sheet_name: str
    data_owner: str
    country: str

class ETLJobExecutor:
    """Main ETL job executor class."""
    
    def __init__(self, job_name: str, secret_name: str, env: str):
        """Initialize ETL job executor.
        
        Args:
            job_name: Name of the Glue job
            secret_name: Name of the secret containing database credentials
            env: Environment name (dev/prod)
        """
        self.job_name = job_name
        self.secret_name = secret_name
        self.env = env
        self.logger = ETLLogger("FileStandardizationJob")
        self.metrics = MetricsCollector()
        
        # Initialize Spark
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark: SparkSession = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(job_name, {'--job-name': job_name})
        
        # Initialize components
        self.db_connection = DatabaseConnection(secret_name)
        self.db_operations = DatabaseOperations(self.db_connection)
        self.extractor = DataExtractor(env, self.logger)
        self.transformer = DataTransformer(self.logger)
        self.loader = DataLoader(self.db_connection, self.logger)

    def process_entity(self, entity_row: Any, file_row: Any) -> bool:
        """Process a single entity.
        
        Args:
            entity_row: Entity details row
            file_row: File information row
            
        Returns:
            bool: True if processing was successful
        """
        try:
            self.logger.log_step(f"Processing entity: {entity_row.data_owner}")
            start_time = datetime.now()
            
            # Get attribute details
            attribute_details = self.db_operations.get_attribute_details(
                entity_row.data_owner,
                entity_row.context,
                entity_row.entity_file_table_name,
                entity_row.entity_sheet_name
            ).collect()
            
            # Extract data
            file_info = FileInfo(
                file_path=file_row.file,
                file_type=file_row.file_extension,
                sheet_name=file_row.daq_sheet_name,
                data_owner=entity_row.data_owner,
                country=entity_row.country
            )
            raw_data = self.extractor.extract_data(file_info)
            
            if raw_data is None:
                return False
            
            # Transform and load data based on context
            success = False
            if entity_row.context.upper() == 'STOCK':
                processed_data = self.transformer.transform_stock_data(raw_data, attribute_details)
                if processed_data:
                    success = self.loader.load_stock_data(processed_data, f"temp_{file_row.id}")
            else:  # SALES
                processed_data = self.transformer.transform_sales_data(raw_data, attribute_details)
                if processed_data:
                    success = self.loader.load_sales_data(processed_data, f"temp_{file_row.id}")
            
            if success:
                # Insert load info
                self.db_operations.insert_temp_load_info(
                    f"temp_{file_row.id}",
                    entity_row.context,
                    entity_row.frequency,
                    1 if entity_row.context.upper() == 'SALES' else 0,
                    1
                )
                
                # Record metrics
                processing_time = (datetime.now() - start_time).total_seconds()
                self.metrics.record_entity_processing(
                    entity_row.data_owner,
                    entity_row.context,
                    processing_time
                )
                
            return success
            
        except Exception as e:
            self.logger.log_error(
                f"Error processing entity {entity_row.data_owner}: {str(e)}",
                exc_info=e
            )
            return False

    def process_file(self, file_row: Any) -> bool:
        """Process a single file.
        
        Args:
            file_row: File information row
            
        Returns:
            bool: True if processing was successful
        """
        try:
            self.logger.log_step(f"Processing file: {file_row.file}")
            start_time = datetime.now()
            
            # Get entity details using sequential matching
            entity_details = self.db_operations.get_entity_details(
                daq_sheet_name=file_row.daq_sheet_name,
                sender_address=file_row.sender_address,
                file_extension=file_row.file_extension,
                receiver_address=file_row.receiver_address,
                file_id=file_row.id
            )
            
            if not entity_details.count():
                self.logger.log_warning(f"No entity details found for file: {file_row.file}")
                return False
            
            # Process each entity
            success = False
            for entity_row in entity_details.collect():
                if self.process_entity(entity_row, file_row):
                    success = True
            
            if success:
                # Mark file as processed
                self.db_operations.mark_file_as_processed(file_row.id)
                
                # Record metrics
                processing_time = (datetime.now() - start_time).total_seconds()
                self.metrics.record_file_processing(
                    file_row.file,
                    processing_time,
                    entity_details.count()
                )
                
            return success
            
        except Exception as e:
            self.logger.log_error(
                f"Error processing file {file_row.file}: {str(e)}",
                exc_info=e
            )
            return False

    def run(self) -> None:
        """Run the ETL job."""
        self.logger.start_job()
        start_time = datetime.now()
        
        try:
            self.logger.log_info(f"Environment: {self.env}")
            
            # Get unprocessed files
            self.logger.log_step("Fetching unprocessed files")
            unprocessed_files = self.db_operations.get_unprocessed_files()
            
            if not unprocessed_files.count():
                self.logger.log_info("No unprocessed files found")
                return
            
            # Process each file
            files_processed = 0
            for file_row in unprocessed_files.collect():
                if self.process_file(file_row):
                    files_processed += 1
            
            # Cleanup
            self.loader.cleanup_temp_tables()
            
            # Record final metrics
            total_time = (datetime.now() - start_time).total_seconds()
            self.metrics.record_job_completion(
                total_time,
                files_processed,
                unprocessed_files.count()
            )
            
            self.logger.end_job()
            self.job.commit()
            
        except Exception as e:
            self.logger.log_error("Job failed", exc_info=e)
            self.logger.end_job("failed")
            raise
        finally:
            self.metrics.flush()

def run_etl_job() -> None:
    """Main ETL job execution function."""
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secret_name', 'env'])
    executor = ETLJobExecutor(args['JOB_NAME'], args['secret_name'], args['env'])
    executor.run()

if __name__ == "__main__":
    run_etl_job() 