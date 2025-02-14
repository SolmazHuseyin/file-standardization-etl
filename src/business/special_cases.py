"""
Special case handling for specific organizations and countries.
"""

from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from decimal import Decimal

class QuimicaSuizaHandler:
    """Handler for Quimica Suiza specific processing."""
    
    @staticmethod
    def process_stock_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Process stock data for Quimica Suiza.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        
        # Fill NULL values in BATCH_NUMBER with 'NA'
        if 'BATCH_NUMBER' in df.columns:
            df['BATCH_NUMBER'] = df['BATCH_NUMBER'].fillna('NA')
        
        # Process INVENTORY_CATEGORY based on BRANCH_NAME
        if 'BRANCH_NAME' in df.columns:
            df['INVENTORY_CATEGORY'] = df['BRANCH_NAME'].apply(
                lambda x: 'PR' if 'PRIVATE' in str(x).upper()
                else ('PU' if 'PUBLIC' in str(x).upper() else 'GN')
            )
        
        # Calculate BLOCKED_QUANTITY from transit quantities
        transit_cols = ['QUANTITY_IN_TRANSIT1', 'QUANTITY_IN_TRANSIT2']
        if all(col in df.columns for col in transit_cols):
            df['BLOCKED_QUANTITY'] = (
                pd.to_numeric(df['QUANTITY_IN_TRANSIT1'], errors='coerce').fillna(0) +
                pd.to_numeric(df['QUANTITY_IN_TRANSIT2'], errors='coerce').fillna(0)
            ) * 1000
        
        # Add missing header row handling
        if len(df.columns) == len(df.iloc[0].unique()):
            empty_row = pd.DataFrame([{col: None for col in df.columns}])
            df = pd.concat([empty_row, df], ignore_index=True)
        
        return df
    
    @staticmethod
    def process_sales_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Process sales data for Quimica Suiza.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        
        # Process SALES_CATEGORY based on BRANCH_NAME
        if 'BRANCH_NAME' in df.columns:
            df['SALES_CATEGORY'] = df['BRANCH_NAME'].apply(
                lambda x: 'PR' if 'PRIVATE' in str(x).upper()
                else ('PU' if 'PUBLIC' in str(x).upper() else 'GN')
            )
        
        # Scale quantity columns
        quantity_cols = ['SALES_QUANTITY', 'RETURN_QUANTITY']
        for col in quantity_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) * 1000
        
        # Scale value columns
        value_cols = ['SALES_VALUE', 'RETURN_VALUE']
        for col in value_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0) * 1000
        
        return df

class RafedHandler:
    """Handler for RAFED specific processing."""
    
    @staticmethod
    def process_organization(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """
        Process organization name based on sheet name.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            sheet_name (str): Sheet name
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        
        if sheet_name.lower() == 'tawam data':
            df['ORGANIZATION_NAME'] = 'TAWAM'
            if 'SALES_QUANTITY' in df.columns:
                df['SALES_QUANTITY'] = df['SALES_QUANTITY'] * -1
        elif sheet_name.lower() == 'mafraq-ssmc data':
            df['ORGANIZATION_NAME'] = 'SSMC'
        
        return df

class CitypharmacyHandler:
    """Handler for Citypharmacy specific processing."""
    
    @staticmethod
    def process_branch_name(df: pd.DataFrame, country: str) -> pd.DataFrame:
        """
        Process branch name based on region.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            country (str): Country name
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        
        if 'REGION_NAME' in df.columns and country == 'UAE':
            df['REGION_NAME'] = df['REGION_NAME'].str.upper()
            df['BRANCH_NAME'] = df.apply(
                lambda row: (
                    f"{row['ORGANIZATION_NAME']} KIZAD"
                    if pd.notna(row['REGION_NAME']) and row['REGION_NAME'] in ['ABU DHABI', 'AL AIN']
                    else (
                        f"{row['ORGANIZATION_NAME']} SHARJAH"
                        if pd.notna(row['REGION_NAME'])
                        else f"{row['ORGANIZATION_NAME']} SHARJAH"
                    )
                ),
                axis=1
            )
            df = df.drop(columns=['REGION_NAME'])
        
        return df
    
    @staticmethod
    def process_categories(df: pd.DataFrame, context: str) -> pd.DataFrame:
        """
        Process inventory/sales categories.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            context (str): Processing context (STOCK/SALES)
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        df = df.copy()
        
        if context == 'STOCK':
            if 'STOCK_CATEGORY' in df.columns:
                df['INVENTORY_CATEGORY'] = df['STOCK_CATEGORY'].apply(
                    lambda x: 'PU' if x == 'I' else ('PR' if x == 'N' else 'GN')
                )
                df = df.drop(columns=['STOCK_CATEGORY'])
        
        elif context == 'SALES':
            if 'SALES_CATEGORY' in df.columns:
                df['SALES_CATEGORY'] = df['SALES_CATEGORY'].apply(
                    lambda x: 'PU' if x == 'I' else ('PR' if x == 'N' else 'GN')
                )
        
        return df

class KuwaitHandler:
    """Handler for Kuwait specific processing."""
    
    @staticmethod
    def split_tilde_rows(df: pd.DataFrame) -> pd.DataFrame:
        """
        Split rows containing tilde (~) character.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame with split rows
        """
        df = df.copy()
        new_rows = []
        
        for _, row in df.iterrows():
            # Find columns containing tilde
            tilde_cols = [col for col in df.columns if '~' in str(row[col])]
            
            if not tilde_cols:
                new_rows.append(row)
                continue
            
            # Find maximum number of splits needed
            max_splits = max(len(str(row[col]).split('~')) for col in tilde_cols)
            
            # Create new rows for each split
            for i in range(max_splits):
                new_row = row.copy()
                for col in df.columns:
                    if col in tilde_cols:
                        splits = str(row[col]).split('~')
                        new_row[col] = splits[i] if i < len(splits) else None
                    else:
                        # Handle numeric values
                        if str(row[col]).isdigit() and str(col) != '0':
                            new_row[col] = row[col] if i == 0 else None
                new_rows.append(new_row)
        
        return pd.DataFrame(new_rows) 