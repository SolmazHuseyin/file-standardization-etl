"""
Stock data model for handling inventory data.
"""

import pandas as pd
from config.settings import STOCK_SCHEMA

class StockData:
    def __init__(self):
        """Initialize an empty stock DataFrame with the predefined schema."""
        self.data = pd.DataFrame({col: pd.Series(dtype=dtype) 
                                for col, dtype in STOCK_SCHEMA.items()})

    def add_record(self, record_dict):
        """
        Add a record to the stock DataFrame.
        
        Args:
            record_dict (dict): Dictionary containing stock record data
        """
        # Convert to DataFrame with single row
        record_df = pd.DataFrame([record_dict])
        
        # Ensure data types match schema
        for col, dtype in STOCK_SCHEMA.items():
            if col in record_df.columns:
                record_df[col] = record_df[col].astype(dtype)
        
        # Append to existing data
        self.data = pd.concat([self.data, record_df], ignore_index=True)

    def add_records(self, records_list):
        """
        Add multiple records to the stock DataFrame.
        
        Args:
            records_list (list): List of dictionaries containing stock records
        """
        for record in records_list:
            self.add_record(record)

    def validate_data(self):
        """
        Validate the data in the DataFrame.
        
        Returns:
            bool: True if data is valid, False otherwise
        """
        # Check for required columns
        required_columns = [
            'DATA_DATE',
            'COUNTRY_NAME',
            'ORGANIZATION_NAME',
            'PRODUCT_ID',
            'AVAILABLE_QUANTITY'
        ]
        
        missing_columns = [col for col in required_columns 
                         if col not in self.data.columns]
        
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for null values in required fields
        null_counts = self.data[required_columns].isnull().sum()
        if null_counts.any():
            print("Null values found in required columns:")
            print(null_counts[null_counts > 0])
            return False
        
        # Validate numeric fields
        numeric_columns = ['AVAILABLE_QUANTITY', 'BLOCKED_QUANTITY']
        for col in numeric_columns:
            if col in self.data.columns:
                if not pd.to_numeric(self.data[col], errors='coerce').notnull().all():
                    print(f"Invalid numeric values found in {col}")
                    return False
        
        return True

    def clean_data(self):
        """Clean and standardize the data in the DataFrame."""
        # Remove any leading/trailing whitespace
        string_columns = [col for col, dtype in STOCK_SCHEMA.items() 
                         if dtype == 'object']
        for col in string_columns:
            if col in self.data.columns:
                self.data[col] = self.data[col].str.strip()
        
        # Convert quantities to integers
        quantity_columns = ['AVAILABLE_QUANTITY', 'BLOCKED_QUANTITY']
        for col in quantity_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                self.data[col] = self.data[col].fillna(0).astype('Int64')
        
        # Standardize date formats
        date_columns = ['DATA_DATE', 'EXPIRY_DATE']
        for col in date_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_datetime(self.data[col], errors='coerce')

    def to_spark_df(self, spark):
        """
        Convert pandas DataFrame to Spark DataFrame.
        
        Args:
            spark: SparkSession instance
            
        Returns:
            DataFrame: Spark DataFrame
        """
        return spark.createDataFrame(self.data)

    def validate_quantities(self):
        """Add quantity validation"""
        self.data['AVAILABLE_QUANTITY'] = pd.to_numeric(
            self.data['AVAILABLE_QUANTITY'], 
            errors='coerce'
        ).fillna(0)
        self.data = self.data[self.data['AVAILABLE_QUANTITY'] >= 0]
