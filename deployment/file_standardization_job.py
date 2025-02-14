"""
Main ETL job script for file standardization.
"""

import sys
import os

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

from src.database.connection import DatabaseConnection
from src.database.operations import DatabaseOperations
from src.etl.extractors import DataExtractor
from src.etl.transformers import DataTransformer
from src.etl.loaders import DataLoader
from src.utils.logging_utils import ETLLogger

def run_etl_job():
    """Main ETL job execution function."""
    
    # Initialize logging
    logger = ETLLogger("FileStandardizationJob")
    logger.start_job()
    
    try:
        # Get job parameters
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secret_name', 'env'])
        
        # Initialize Spark context
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)
        
        logger.log_info(f"Environment: {args['env']}")
        
        # Initialize components
        db_connection = DatabaseConnection(args['secret_name'])
        db_operations = DatabaseOperations(db_connection)
        extractor = DataExtractor(args['env'], logger)
        transformer = DataTransformer(logger)
        loader = DataLoader(db_connection, logger)
        
        # Get unprocessed files
        logger.log_step("Fetching unprocessed files")
        unprocessed_files = db_operations.get_unprocessed_files()
        
        if not unprocessed_files.count():
            logger.log_info("No unprocessed files found")
            return
        
        # Process each file
        for file_row in unprocessed_files.collect():
            try:
                logger.log_step(f"Processing file: {file_row.file}")
                
                # Get entity details using sequential matching
                entity_details = db_operations.get_entity_details(
                    daq_sheet_name=file_row.daq_sheet_name,
                    sender_address=file_row.sender_address,
                    file_extension=file_row.file_extension,
                    receiver_address=file_row.receiver_address,
                    file_id=file_row.id
                )
                
                if not entity_details.count():
                    logger.log_warning(f"No entity details found for file: {file_row.file}")
                    continue
                
                # Process each entity
                for entity_row in entity_details.collect():
                    try:
                        logger.log_step(f"Processing entity: {entity_row.data_owner}")
                        
                        # Get attribute details
                        attribute_details = db_operations.get_attribute_details(
                            entity_row.data_owner,
                            entity_row.context,
                            entity_row.entity_file_table_name,
                            entity_row.entity_sheet_name
                        ).collect()
                        
                        # Extract data
                        file_info = {
                            'file_path': file_row.file,
                            'file_type': file_row.file_extension,
                            'sheet_name': file_row.daq_sheet_name,
                            'data_owner': entity_row.data_owner,
                            'country': entity_row.country
                        }
                        raw_data = extractor.extract_data(file_info)
                        
                        if raw_data is None:
                            continue
                        
                        # Transform data based on context
                        if entity_row.context.upper() == 'STOCK':
                            processed_data = transformer.transform_stock_data(
                                raw_data,
                                attribute_details
                            )
                            if processed_data:
                                success = loader.load_stock_data(
                                    processed_data,
                                    f"temp_{file_row.id}"
                                )
                        else:  # SALES
                            processed_data = transformer.transform_sales_data(
                                raw_data,
                                attribute_details
                            )
                            if processed_data:
                                success = loader.load_sales_data(
                                    processed_data,
                                    f"temp_{file_row.id}"
                                )
                        
                        if success:
                            # Insert load info
                            db_operations.insert_temp_load_info(
                                f"temp_{file_row.id}",
                                entity_row.context,
                                entity_row.frequency,
                                1 if entity_row.context.upper() == 'SALES' else 0,
                                1
                            )
                    
                    except Exception as entity_error:
                        logger.log_error(
                            f"Error processing entity {entity_row.data_owner}: {str(entity_error)}",
                            exc_info=entity_error
                        )
                        continue
                
                # Mark file as processed
                db_operations.mark_file_as_processed(file_row.id)
                
            except Exception as file_error:
                logger.log_error(
                    f"Error processing file {file_row.file}: {str(file_error)}",
                    exc_info=file_error
                )
                continue
        
        # Cleanup
        loader.cleanup_temp_tables()
        
        logger.end_job()
        job.commit()
        
    except Exception as e:
        logger.log_error("Job failed", exc_info=e)
        logger.end_job("failed")
        raise
    
if __name__ == "__main__":
    run_etl_job() 