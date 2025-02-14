"""
Business rules for data validation and transformation.
"""

from typing import Dict, List, Any
import pandas as pd
import numpy as np

class DataOwnerRules:
    """Rules for specific data owners."""
    
    @staticmethod
    def validate_alliance(data_owner: str, country: str, sheet_name: str) -> bool:
        """
        Validate ALLIANCE data.
        
        Args:
            data_owner (str): Data owner name
            country (str): Country name
            sheet_name (str): Sheet name
            
        Returns:
            bool: True if valid, False otherwise
        """
        return (
            data_owner == 'ALLIANCE' 
            and country == 'TURKIYE' 
            and sheet_name.lower() == 'urundepobazinda'
        )
    
    @staticmethod
    def validate_rafed(data_owner: str, country: str, sheet_name: str) -> bool:
        """
        Validate RAFED data.
        
        Args:
            data_owner (str): Data owner name
            country (str): Country name
            sheet_name (str): Sheet name
            
        Returns:
            bool: True if valid, False otherwise
        """
        valid_sheets = ['mafraq-ssmc data', 'tawam data']
        return (
            data_owner == 'RAFED'
            and country == 'UAE'
            and sheet_name.lower() in valid_sheets
        )

class TransformationRules:
    """Rules for data transformations."""
    
    @staticmethod
    def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize column names.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with cleaned column names
        """
        df = df.copy()
        df.columns = (
            df.columns.str.strip()
            .str.replace(r'[^\w\s]', '', regex=True)
            .str.replace(r'\s+', '_', regex=True)
            .str.upper()
        )
        return df
    
    @staticmethod
    def remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove empty columns from DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with empty columns removed
        """
        df = df.copy()
        for col in df.columns:
            if df[col].astype(str).str.strip().eq('').all():
                df = df.drop(columns=[col])
        return df
    
    @staticmethod
    def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove empty rows from DataFrame.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with empty rows removed
        """
        df = df.copy()
        return df.dropna(how='all').reset_index(drop=True)

class ValidationRules:
    """Rules for data validation."""
    
    @staticmethod
    def validate_mandatory_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate presence of mandatory columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            required_columns (List[str]): List of required column names
            
        Returns:
            bool: True if all required columns present, False otherwise
        """
        return all(col in df.columns for col in required_columns)
    
    @staticmethod
    def validate_numeric_columns(df: pd.DataFrame, numeric_columns: List[str]) -> bool:
        """
        Validate numeric columns.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            numeric_columns (List[str]): List of numeric column names
            
        Returns:
            bool: True if all numeric columns are valid, False otherwise
        """
        for col in numeric_columns:
            if col in df.columns:
                if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                    return False
        return True

class SpecialCaseRules:
    """Rules for special cases."""
    
    @staticmethod
    def process_kuwait_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Process data for Kuwait.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        new_rows = []
        
        for _, row in df.iterrows():
            # Find columns containing '~'
            split_cols = [col for col in df.columns if '~' in str(row[col])]
            
            if not split_cols:
                new_rows.append(row)
                continue
            
            # Find max splits needed
            max_splits = max(len(str(row[col]).split('~')) for col in split_cols)
            
            # Process splits
            for i in range(max_splits):
                new_row = row.copy()
                for col in df.columns:
                    if col in split_cols:
                        splits = str(row[col]).split('~')
                        new_row[col] = splits[i] if i < len(splits) else None
                new_rows.append(new_row)
        
        return pd.DataFrame(new_rows)
    
    @staticmethod
    def process_citypharmacy_data(df: pd.DataFrame, context: str) -> pd.DataFrame:
        """
        Process data for Citypharmacy.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            context (str): Processing context (STOCK/SALES)
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        
        if context == 'STOCK':
            # Filter out 'I' category
            if 'STOCK_CATEGORY' in df.columns:
                df = df[df['STOCK_CATEGORY'] != 'I']
            
            # Update inventory category
            if 'INVENTORY_CATEGORY' in df.columns:
                df['INVENTORY_CATEGORY'] = df.apply(
                    lambda row: 'PU' if row.get('STOCK_CATEGORY') == 'I'
                    else ('PR' if row.get('STOCK_CATEGORY') == 'N' else 'GN'),
                    axis=1
                )
        
        elif context == 'SALES':
            # Update sales category
            if 'SALES_CATEGORY' in df.columns:
                df['SALES_CATEGORY'] = df.apply(
                    lambda row: 'PU' if row.get('SALES_CATEGORY') == 'I'
                    else ('PR' if row.get('SALES_CATEGORY') == 'N' else 'GN'),
                    axis=1
                )
        
        return df 

class BusinessRules:
    def apply_organization_rules(self, df, data_owner):
        """Add organization-specific rules"""
        if data_owner == 'ALLIANCE':
            df = self.handle_alliance_data(df)
        elif data_owner == 'RAFED':
            df = self.handle_rafed_data(df)
        return df 