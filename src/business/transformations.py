"""
Business-specific data transformations.
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from decimal import Decimal
from datetime import datetime, date

class StockTransformations:
    """Transformations for stock data."""
    
    @staticmethod
    def standardize_quantities(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize quantity columns in stock data.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized quantities
        """
        df = df.copy()
        quantity_columns = ['AVAILABLE_QUANTITY', 'BLOCKED_QUANTITY']
        
        for col in quantity_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype('Int64')
        
        return df
    
    @staticmethod
    def process_inventory_category(df: pd.DataFrame, data_owner: str) -> pd.DataFrame:
        """
        Process inventory category based on data owner rules.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            data_owner (str): Data owner name
            
        Returns:
            pd.DataFrame: DataFrame with processed inventory category
        """
        df = df.copy()
        
        if 'INVENTORY_CATEGORY' not in df.columns:
            df['INVENTORY_CATEGORY'] = 'GN'
            
        if data_owner == 'QUIMICA SUIZA':
            if 'BRANCH_NAME' in df.columns:
                df['INVENTORY_CATEGORY'] = df['BRANCH_NAME'].apply(
                    lambda x: 'PR' if 'PRIVATE' in str(x).upper()
                    else ('PU' if 'PUBLIC' in str(x).upper() else 'GN')
                )
        
        return df

class SalesTransformations:
    """Transformations for sales data."""
    
    @staticmethod
    def standardize_quantities(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize quantity columns in sales data.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized quantities
        """
        df = df.copy()
        
        # Process sales quantity
        if 'SALES_QUANTITY' in df.columns:
            df['SALES_QUANTITY'] = pd.to_numeric(df['SALES_QUANTITY'], errors='coerce')
            df['SALES_QUANTITY'] = df['SALES_QUANTITY'].fillna(0).astype('Int64')
        
        # Process return quantity
        if 'RETURN_QUANTITY' in df.columns:
            df['RETURN_QUANTITY'] = pd.to_numeric(df['RETURN_QUANTITY'], errors='coerce')
            df['RETURN_QUANTITY'] = df['RETURN_QUANTITY'].fillna(0).astype('Int64')
        
        return df
    
    @staticmethod
    def standardize_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize monetary value columns in sales data.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized values
        """
        df = df.copy()
        value_columns = ['SALES_VALUE', 'RETURN_VALUE']
        
        for col in value_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype('float64')
                df[col] = df[col].apply(Decimal)
        
        return df
    
    @staticmethod
    def process_sales_category(df: pd.DataFrame, data_owner: str) -> pd.DataFrame:
        """
        Process sales category based on data owner rules.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            data_owner (str): Data owner name
            
        Returns:
            pd.DataFrame: DataFrame with processed sales category
        """
        df = df.copy()
        
        if 'SALES_CATEGORY' not in df.columns:
            df['SALES_CATEGORY'] = 'GN'
            
        if data_owner == 'QUIMICA SUIZA':
            if 'BRANCH_NAME' in df.columns:
                df['SALES_CATEGORY'] = df['BRANCH_NAME'].apply(
                    lambda x: 'PR' if 'PRIVATE' in str(x).upper()
                    else ('PU' if 'PUBLIC' in str(x).upper() else 'GN')
                )
        
        return df

class DateTransformations:
    """Transformations for date fields."""
    
    @staticmethod
    def standardize_dates(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """
        Standardize date columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            date_columns (List[str]): List of date column names
            
        Returns:
            pd.DataFrame: DataFrame with standardized dates
        """
        df = df.copy()
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].fillna(pd.Timestamp('2199-12-31'))
        
        return df
    
    @staticmethod
    def process_data_date(df: pd.DataFrame, frequency: str, mail_date: datetime) -> pd.DataFrame:
        """
        Process DATA_DATE based on frequency.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            frequency (str): Data frequency (MONTHLY/WEEKLY/DAILY)
            mail_date (datetime): Mail date
            
        Returns:
            pd.DataFrame: DataFrame with processed DATA_DATE
        """
        df = df.copy()
        
        if 'DATA_DATE' not in df.columns:
            if frequency == 'MONTHLY':
                # Set to last day of previous month
                first_day = date(mail_date.year, mail_date.month, 1)
                last_day = first_day - pd.Timedelta(days=1)
                df['DATA_DATE'] = last_day
            else:
                df['DATA_DATE'] = mail_date
        
        return df 