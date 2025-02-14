"""
Data transformation module for the ETL job.
"""

import pandas as pd
import numpy as np
from src.utils.date_utils import parse_date, get_last_day_of_month
from src.models.stock import StockData
from src.models.sales import SalesData
from typing import List

class DataTransformer:
    def __init__(self, logger):
        """
        Initialize the data transformer.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger

    def clean_column_names(self, df):
        """
        Clean and standardize column names.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: DataFrame with cleaned column names
        """
        try:
            # Remove special characters and standardize spacing
            df.columns = df.columns.str.strip()
            df.columns = df.columns.str.replace(r'[^\w\s]', '', regex=True)
            df.columns = df.columns.str.replace(r'\s+', '_', regex=True)
            df.columns = df.columns.str.upper()
            
            self.logger.log_info("Column names cleaned successfully")
            return df
            
        except Exception as e:
            self.logger.log_error(f"Error cleaning column names: {str(e)}", exc_info=e)
            return df

    def map_columns(self, df, column_mapping):
        """
        Map DataFrame columns according to provided mapping.
        
        Args:
            df (DataFrame): Input DataFrame
            column_mapping (dict): Dictionary mapping original to new column names
            
        Returns:
            DataFrame: DataFrame with mapped columns
        """
        try:
            # Create a copy to avoid modifying the original
            df_mapped = df.copy()
            
            # Rename columns according to mapping
            df_mapped.rename(columns=column_mapping, inplace=True)
            
            # Log mapped columns
            self.logger.log_info("Columns mapped successfully")
            self.logger.log_debug(f"Column mapping: {column_mapping}")
            
            return df_mapped
            
        except Exception as e:
            self.logger.log_error(f"Error mapping columns: {str(e)}", exc_info=e)
            return df

    def transform_stock_data(self, df, attribute_details):
        """
        Transform raw data into stock data format.
        
        Args:
            df (DataFrame): Raw input DataFrame
            attribute_details (DataFrame): DataFrame containing attribute mapping details
            
        Returns:
            StockData: Transformed stock data
        """
        try:
            # Initialize stock data model
            stock_data = StockData()
            
            # Clean and map columns
            df = self.clean_column_names(df)
            
            # Create column mapping from attribute details
            column_mapping = {
                attr['original_column_name']: attr['etl_column_name']
                for attr in attribute_details
                if attr['original_column_name'] in df.columns
            }
            
            df = self.map_columns(df, column_mapping)
            
            # Convert data types
            for col in df.columns:
                if col in ['AVAILABLE_QUANTITY', 'BLOCKED_QUANTITY']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif col in ['DATA_DATE', 'EXPIRY_DATE']:
                    df[col] = df[col].apply(parse_date)
            
            # Add records to stock data model
            stock_data.add_records(df.to_dict('records'))
            
            # Validate and clean data
            if stock_data.validate_data():
                stock_data.clean_data()
                self.logger.log_info("Stock data transformed successfully")
                return stock_data
            else:
                self.logger.log_error("Stock data validation failed")
                return None
            
        except Exception as e:
            self.logger.log_error(f"Error transforming stock data: {str(e)}", exc_info=e)
            return None

    def transform_sales_data(self, df, attribute_details):
        """
        Transform raw data into sales data format.
        
        Args:
            df (DataFrame): Raw input DataFrame
            attribute_details (dict): Dictionary containing attribute mapping details
            
        Returns:
            SalesData: Transformed sales data
        """
        try:
            # Initialize sales data model
            sales_data = SalesData()
            
            # Clean and map columns
            df = self.clean_column_names(df)
            column_mapping = {
                attr['original_column_name']: attr['etl_column_name']
                for attr in attribute_details
                if attr['original_column_name'] in df.columns
            }
            df = self.map_columns(df, column_mapping)
            
            # Convert data types
            for col in df.columns:
                if col in ['SALES_QUANTITY', 'RETURN_QUANTITY']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif col in ['SALES_VALUE', 'RETURN_VALUE']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif col in ['DATA_DATE', 'INVOICE_DATE']:
                    df[col] = df[col].apply(parse_date)
            
            # Add records to sales data model
            sales_data.add_records(df.to_dict('records'))
            
            # Validate and clean data
            if sales_data.validate_data():
                sales_data.clean_data()
                self.logger.log_info("Sales data transformed successfully")
                return sales_data
            else:
                self.logger.log_error("Sales data validation failed")
                return None
            
        except Exception as e:
            self.logger.log_error(f"Error transforming sales data: {str(e)}", exc_info=e)
            return None

    def remove_unwanted_characters(self, df, columns):
        """
        Remove unwanted characters from specified columns.
        
        Args:
            df (DataFrame): Input DataFrame
            columns (list): List of columns to clean
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        try:
            df_clean = df.copy()
            for col in columns:
                if col in df_clean.columns:
                    # Remove special characters and extra spaces
                    df_clean[col] = df_clean[col].astype(str)
                    df_clean[col] = df_clean[col].str.replace(r'[^\w\s-]', '', regex=True)
                    df_clean[col] = df_clean[col].str.strip()
            
            self.logger.log_info(f"Cleaned unwanted characters from columns: {columns}")
            return df_clean
            
        except Exception as e:
            self.logger.log_error(f"Error removing unwanted characters: {str(e)}", exc_info=e)
            return df

    def handle_special_values(self, df):
        """Add SURGIPHARM special case handling"""
        if self.data_owner == 'SURGIPHARM' and self.country == 'KENYA':
            df.loc[df['BRANCH_NAME'].str.startswith('MAINSTORES'), 'BRANCH_NAME'] = None
            df.loc[df['SALES_CATEGORY'] == 'SALESSTOCK', 'SALES_CATEGORY'] = None

    def process_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """
        Process date columns in DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            date_columns (List[str]): List of date column names
            
        Returns:
            pd.DataFrame: DataFrame with processed dates
        """
        df = df.copy()
        
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(parse_date)
                
                # Handle special cases
                if col == 'DATA_DATE' and self.frequency == 'MONTHLY':
                    df[col] = df[col].apply(
                        lambda x: get_last_day_of_month(x) if x else None
                    )
                    
        return df
