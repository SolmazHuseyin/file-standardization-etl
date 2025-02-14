"""
Data extraction module for the ETL job.
"""

import pandas as pd
import s3fs
from config.settings import S3_BUCKET_PREFIX, SPECIAL_CASES

class DataExtractor:
    def __init__(self, s3_connection, logger):
        """
        Initialize the data extractor.
        
        Args:
            s3_connection: S3 connection object
            logger: Logger instance
        """
        self.s3 = s3_connection
        self.logger = logger
        self.fs = s3fs.S3FileSystem(anon=False)

    def read_excel_file(self, file_path, sheet_name=None):
        """
        Read data from an Excel file.
        
        Args:
            file_path (str): Path to the Excel file
            sheet_name (str, optional): Name of the sheet to read
            
        Returns:
            DataFrame: pandas DataFrame with the file contents
        """
        try:
            with self.fs.open(file_path, 'rb') as f:
                if sheet_name:
                    df = pd.read_excel(f, sheet_name=sheet_name)
                else:
                    df = pd.read_excel(f)
                
            self.logger.log_info(f"Successfully read Excel file: {file_path}")
            self.logger.log_dataframe_info(df, "Excel Data")
            return df
            
        except Exception as e:
            self.logger.log_error(f"Error reading Excel file {file_path}: {str(e)}", exc_info=e)
            return None

    def read_csv_file(self, file_path, **kwargs):
        """
        Read data from a CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            **kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame: pandas DataFrame with the file contents
        """
        try:
            with self.fs.open(file_path, 'rb') as f:
                df = pd.read_csv(f, **kwargs)
                
            self.logger.log_info(f"Successfully read CSV file: {file_path}")
            self.logger.log_dataframe_info(df, "CSV Data")
            return df
            
        except Exception as e:
            self.logger.log_error(f"Error reading CSV file {file_path}: {str(e)}", exc_info=e)
            return None

    def validate_file_source(self, data_owner, country, sheet_name):
        """
        Validate file source based on special cases.
        
        Args:
            data_owner (str): Data owner name
            country (str): Country name
            sheet_name (str): Sheet name
            
        Returns:
            bool: True if source is valid, False otherwise
        """
        if data_owner.upper() in SPECIAL_CASES:
            case = SPECIAL_CASES[data_owner.upper()]
            
            # Check country match
            if case['country'] != country.upper():
                self.logger.log_warning(
                    f"Invalid country for {data_owner}: {country}"
                )
                return False
            
            # Check sheet name constraints
            if 'sheet_name' in case and case['sheet_name'].lower() != sheet_name.lower():
                self.logger.log_warning(
                    f"Invalid sheet name for {data_owner}: {sheet_name}"
                )
                return False
                
            if 'valid_sheets' in case and sheet_name.lower() not in case['valid_sheets']:
                self.logger.log_warning(
                    f"Sheet name {sheet_name} not in valid sheets for {data_owner}"
                )
                return False
        
        return True

    def extract_data(self, file_info):
        """
        Extract data based on file information.
        
        Args:
            file_info (dict): Dictionary containing file information
                Required keys:
                - file_path: Path to the file
                - file_type: Type of file (excel/csv)
                - sheet_name: Sheet name (for Excel files)
                - data_owner: Data owner name
                - country: Country name
            
        Returns:
            DataFrame: pandas DataFrame with the extracted data
        """
        # Validate required keys
        required_keys = ['file_path', 'file_type', 'data_owner', 'country']
        missing_keys = [key for key in required_keys if key not in file_info]
        if missing_keys:
            self.logger.log_error(f"Missing required keys in file_info: {missing_keys}")
            return None
        
        # Validate file source
        sheet_name = file_info.get('sheet_name')
        if not self.validate_file_source(
            file_info['data_owner'],
            file_info['country'],
            sheet_name or ''
        ):
            return None
        
        # Extract data based on file type
        file_path = f"{self.s3_bucket}/{file_info['file_path']}"
        if file_info['file_type'].lower() == 'excel':
            return self.read_excel_file(file_path, sheet_name)
        elif file_info['file_type'].lower() == 'csv':
            return self.read_csv_file(file_path)
        else:
            self.logger.log_error(f"Unsupported file type: {file_info['file_type']}")
            return None
