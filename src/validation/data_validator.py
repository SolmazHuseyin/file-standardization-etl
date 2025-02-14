"""
Business data validation module.
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime

class StockDataValidator:
    """Validator for stock data."""
    
    def __init__(self):
        """Initialize stock data validator."""
        self.errors = []
        self.warnings = []
        
        # Define required columns
        self.required_columns = [
            'DATA_DATE',
            'COUNTRY_NAME',
            'ORGANIZATION_NAME',
            'PRODUCT_ID',
            'AVAILABLE_QUANTITY'
        ]
        
        # Define numeric columns
        self.numeric_columns = [
            'AVAILABLE_QUANTITY',
            'BLOCKED_QUANTITY'
        ]
        
        # Define date columns
        self.date_columns = [
            'DATA_DATE',
            'EXPIRY_DATE'
        ]
        
        # Define categorical columns and their allowed values
        self.categorical_validations = {
            'INVENTORY_CATEGORY': ['GN', 'PR', 'PU']
        }
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate stock data.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        is_valid = True
        
        # Check required columns
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            self.errors.append(f"Missing required columns: {missing_columns}")
            is_valid = False
        
        # Check numeric columns
        for col in self.numeric_columns:
            if col in df.columns:
                non_numeric = df[~pd.to_numeric(df[col], errors='coerce').notnull()][col]
                if not non_numeric.empty:
                    self.errors.append(f"Non-numeric values found in {col}: {non_numeric.unique()}")
                    is_valid = False
        
        # Check date columns
        for col in self.date_columns:
            if col in df.columns:
                non_dates = df[~pd.to_datetime(df[col], errors='coerce').notnull()][col]
                if not non_dates.empty:
                    self.errors.append(f"Invalid dates found in {col}: {non_dates.unique()}")
                    is_valid = False
        
        # Check categorical values
        for col, allowed_values in self.categorical_validations.items():
            if col in df.columns:
                invalid_values = df[df[col].notna()][~df[col].isin(allowed_values)][col].unique()
                if len(invalid_values) > 0:
                    self.errors.append(f"Invalid values in {col}: {invalid_values}")
                    is_valid = False
        
        # Check for negative quantities
        for col in ['AVAILABLE_QUANTITY', 'BLOCKED_QUANTITY']:
            if col in df.columns:
                negative_values = df[df[col] < 0]
                if not negative_values.empty:
                    self.warnings.append(f"Negative values found in {col}")
        
        return is_valid

class SalesDataValidator:
    """Validator for sales data."""
    
    def __init__(self):
        """Initialize sales data validator."""
        self.errors = []
        self.warnings = []
        
        # Define required columns
        self.required_columns = [
            'DATA_DATE',
            'COUNTRY_NAME',
            'ORGANIZATION_NAME',
            'PRODUCT_ID',
            'SALES_QUANTITY',
            'SALES_VALUE'
        ]
        
        # Define numeric columns
        self.numeric_columns = [
            'SALES_QUANTITY',
            'RETURN_QUANTITY',
            'SALES_VALUE',
            'RETURN_VALUE',
            'TAX_IDENTIFICATION_NUMBER'
        ]
        
        # Define date columns
        self.date_columns = [
            'DATA_DATE',
            'INVOICE_DATE'
        ]
        
        # Define categorical columns and their allowed values
        self.categorical_validations = {
            'SALES_CATEGORY': ['GN', 'PR', 'PU']
        }
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate sales data.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        is_valid = True
        
        # Check required columns
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            self.errors.append(f"Missing required columns: {missing_columns}")
            is_valid = False
        
        # Check numeric columns
        for col in self.numeric_columns:
            if col in df.columns:
                non_numeric = df[~pd.to_numeric(df[col], errors='coerce').notnull()][col]
                if not non_numeric.empty:
                    self.errors.append(f"Non-numeric values found in {col}: {non_numeric.unique()}")
                    is_valid = False
        
        # Check date columns
        for col in self.date_columns:
            if col in df.columns:
                non_dates = df[~pd.to_datetime(df[col], errors='coerce').notnull()][col]
                if not non_dates.empty:
                    self.errors.append(f"Invalid dates found in {col}: {non_dates.unique()}")
                    is_valid = False
        
        # Check categorical values
        for col, allowed_values in self.categorical_validations.items():
            if col in df.columns:
                invalid_values = df[df[col].notna()][~df[col].isin(allowed_values)][col].unique()
                if len(invalid_values) > 0:
                    self.errors.append(f"Invalid values in {col}: {invalid_values}")
                    is_valid = False
        
        # Check for negative quantities and values
        if 'SALES_QUANTITY' in df.columns and 'RETURN_QUANTITY' in df.columns:
            net_quantity = df['SALES_QUANTITY'] - df['RETURN_QUANTITY']
            if (net_quantity < 0).any():
                self.warnings.append("Return quantity exceeds sales quantity")
        
        if 'SALES_VALUE' in df.columns and 'RETURN_VALUE' in df.columns:
            net_value = df['SALES_VALUE'] - df['RETURN_VALUE']
            if (net_value < 0).any():
                self.warnings.append("Return value exceeds sales value")
        
        return is_valid

class DataConsistencyValidator:
    """Validator for data consistency across related datasets."""
    
    def validate_stock_sales_consistency(
        self, 
        stock_df: pd.DataFrame, 
        sales_df: pd.DataFrame
    ) -> List[str]:
        """
        Validate consistency between stock and sales data.
        
        Args:
            stock_df (pd.DataFrame): Stock DataFrame
            sales_df (pd.DataFrame): Sales DataFrame
            
        Returns:
            List[str]: List of inconsistency warnings
        """
        warnings = []
        
        # Check product consistency
        stock_products = set(stock_df['PRODUCT_ID'].unique())
        sales_products = set(sales_df['PRODUCT_ID'].unique())
        
        products_in_sales_not_stock = sales_products - stock_products
        if products_in_sales_not_stock:
            warnings.append(
                f"Products in sales but not in stock: {products_in_sales_not_stock}"
            )
        
        # Check organization consistency
        stock_orgs = set(stock_df['ORGANIZATION_NAME'].unique())
        sales_orgs = set(sales_df['ORGANIZATION_NAME'].unique())
        
        orgs_mismatch = stock_orgs.symmetric_difference(sales_orgs)
        if orgs_mismatch:
            warnings.append(
                f"Organizations not matching between stock and sales: {orgs_mismatch}"
            )
        
        return warnings 