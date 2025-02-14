"""
Sales data model for handling sales transaction data.
"""

import pandas as pd
from config.settings import SALES_SCHEMA

class SalesData:
    def __init__(self):
        """Initialize an empty sales DataFrame with the predefined schema."""
        self.data = pd.DataFrame({col: pd.Series(dtype=dtype) 
                                for col, dtype in SALES_SCHEMA.items()})

    def add_record(self, record_dict):
        """
        Add a record to the sales DataFrame.
        
        Args:
            record_dict (dict): Dictionary containing sales record data
        """
        # Convert to DataFrame with single row
        record_df = pd.DataFrame([record_dict])
        
        # Ensure data types match schema
        for col, dtype in SALES_SCHEMA.items():
            if col in record_df.columns:
                record_df[col] = record_df[col].astype(dtype)
        
        # Append to existing data
        self.data = pd.concat([self.data, record_df], ignore_index=True)

    def add_records(self, records_list):
        """
        Add multiple records to the sales DataFrame.
        
        Args:
            records_list (list): List of dictionaries containing sales records
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
            'SALES_QUANTITY',
            'SALES_VALUE'
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
        numeric_columns = ['SALES_QUANTITY', 'RETURN_QUANTITY', 
                         'SALES_VALUE', 'RETURN_VALUE']
        for col in numeric_columns:
            if col in self.data.columns:
                if not pd.to_numeric(self.data[col], errors='coerce').notnull().all():
                    print(f"Invalid numeric values found in {col}")
                    return False
        
        return True

    def clean_data(self):
        """Clean and standardize the data in the DataFrame."""
        # Remove any leading/trailing whitespace
        string_columns = [col for col, dtype in SALES_SCHEMA.items() 
                         if dtype == 'object']
        for col in string_columns:
            if col in self.data.columns:
                self.data[col] = self.data[col].str.strip()
        
        # Convert quantities to integers
        quantity_columns = ['SALES_QUANTITY', 'RETURN_QUANTITY']
        for col in quantity_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                self.data[col] = self.data[col].fillna(0).astype('Int64')
        
        # Convert monetary values to float
        value_columns = ['SALES_VALUE', 'RETURN_VALUE']
        for col in value_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                self.data[col] = self.data[col].fillna(0.0).astype('float64')
        
        # Standardize date formats
        date_columns = ['DATA_DATE', 'INVOICE_DATE']
        for col in date_columns:
            if col in self.data.columns:
                self.data[col] = pd.to_datetime(self.data[col], errors='coerce')

    def calculate_net_sales(self):
        """
        Calculate net sales quantities and values.
        
        Returns:
            tuple: (net_quantity, net_value)
        """
        net_quantity = (self.data['SALES_QUANTITY'].sum() - 
                       self.data['RETURN_QUANTITY'].sum())
        net_value = (self.data['SALES_VALUE'].sum() - 
                    self.data['RETURN_VALUE'].sum())
        return net_quantity, net_value

    def to_spark_df(self, spark):
        """
        Convert pandas DataFrame to Spark DataFrame.
        
        Args:
            spark: SparkSession instance
            
        Returns:
            DataFrame: Spark DataFrame
        """
        return spark.createDataFrame(self.data)
