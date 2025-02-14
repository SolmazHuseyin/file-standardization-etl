"""
Schema validation for data frames.
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from src.utils.date_utils import validate_date_range

class SchemaValidator:
    """Validator for DataFrame schemas."""
    
    def __init__(self, schema: Dict[str, str]):
        """
        Initialize schema validator.
        
        Args:
            schema (Dict[str, str]): Schema dictionary mapping column names to data types
        """
        self.schema = schema
        self.errors = []
    
    def validate_columns(self, df: pd.DataFrame) -> bool:
        """
        Validate presence and types of columns.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        # Check for missing columns
        missing_columns = [col for col in self.schema.keys() if col not in df.columns]
        if missing_columns:
            self.errors.append(f"Missing columns: {missing_columns}")
            return False
        
        # Check column types
        for col, dtype in self.schema.items():
            try:
                if dtype == 'datetime64[ns]':
                    pd.to_datetime(df[col], errors='raise')
                elif dtype == 'Int64':
                    pd.to_numeric(df[col], errors='raise')
                elif dtype == 'float64':
                    pd.to_numeric(df[col], errors='raise')
            except (ValueError, TypeError):
                self.errors.append(f"Invalid type for column {col}. Expected {dtype}")
                return False
        
        return True
    
    def validate_mandatory_values(self, df: pd.DataFrame, mandatory_columns: List[str]) -> bool:
        """
        Validate presence of values in mandatory columns.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            mandatory_columns (List[str]): List of mandatory column names
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        for col in mandatory_columns:
            if col not in df.columns:
                self.errors.append(f"Missing mandatory column: {col}")
                return False
            
            null_count = df[col].isnull().sum()
            if null_count > 0:
                self.errors.append(f"Found {null_count} null values in mandatory column {col}")
                return False
        
        return True
    
    def validate_numeric_ranges(
        self, 
        df: pd.DataFrame, 
        range_validations: Dict[str, Dict[str, float]]
    ) -> bool:
        """
        Validate numeric ranges for specified columns.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            range_validations (Dict[str, Dict[str, float]]): Dictionary mapping column names
                to their min/max range values
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        for col, ranges in range_validations.items():
            if col not in df.columns:
                continue
                
            min_val = ranges.get('min')
            max_val = ranges.get('max')
            
            if min_val is not None and (df[col] < min_val).any():
                self.errors.append(f"Values below minimum {min_val} found in column {col}")
                return False
                
            if max_val is not None and (df[col] > max_val).any():
                self.errors.append(f"Values above maximum {max_val} found in column {col}")
                return False
        
        return True
    
    def validate_date_ranges(
        self, 
        df: pd.DataFrame, 
        date_columns: List[str],
        min_date: Optional[datetime] = None,
        max_date: Optional[datetime] = None
    ) -> bool:
        """
        Validate date ranges for specified columns.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            date_columns (List[str]): List of date column names
            min_date (datetime, optional): Minimum allowed date
            max_date (datetime, optional): Maximum allowed date
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        for col in date_columns:
            if col not in df.columns:
                continue
                
            dates = pd.to_datetime(df[col], errors='coerce')
            
            if min_date is not None and (dates < min_date).any():
                self.errors.append(f"Dates before {min_date} found in column {col}")
                return False
                
            if max_date is not None and (dates > max_date).any():
                self.errors.append(f"Dates after {max_date} found in column {col}")
                return False
        
        return True
    
    def validate_categorical_values(
        self, 
        df: pd.DataFrame, 
        categorical_validations: Dict[str, List[str]]
    ) -> bool:
        """
        Validate categorical values for specified columns.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            categorical_validations (Dict[str, List[str]]): Dictionary mapping column names
                to their allowed values
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        for col, allowed_values in categorical_validations.items():
            if col not in df.columns:
                continue
                
            invalid_values = df[df[col].notna()][~df[col].isin(allowed_values)][col].unique()
            if len(invalid_values) > 0:
                self.errors.append(
                    f"Invalid values found in column {col}: {invalid_values}"
                )
                return False
        
        return True
    
    def get_validation_errors(self) -> List[str]:
        """
        Get list of validation errors.
        
        Returns:
            List[str]: List of error messages
        """
        return self.errors 

    def validate_mandatory_columns(self, df, context):
        """Add column count validation"""
        mandatory_cols = self.get_mandatory_columns(context)
        matching_cols = [col for col in df.columns if col in mandatory_cols]
        return len(matching_cols) == len(mandatory_cols) 

    def validate_dates(self, df: pd.DataFrame, date_columns: Dict[str, Dict]) -> bool:
        """
        Validate date columns in DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            date_columns (Dict[str, Dict]): Dictionary mapping column names to validation rules
            
        Returns:
            bool: True if validation passes
        """
        for col, rules in date_columns.items():
            if col not in df.columns:
                continue
                
            min_date = rules.get('min_date')
            max_date = rules.get('max_date')
            
            invalid_dates = df[~df[col].apply(
                lambda x: validate_date_range(x, min_date, max_date)
            )]
            
            if not invalid_dates.empty:
                self.errors.append(
                    f"Invalid dates found in column {col}"
                )
                return False
                
        return True 