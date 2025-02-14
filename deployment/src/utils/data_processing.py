"""
Utility module for common data processing functions.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean column names by removing whitespace and special characters.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with cleaned column names
    """
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.upper()
    df.columns = df.columns.str.replace(' ', '_')
    return df

def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where all values are null.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with empty rows removed
    """
    return df.dropna(how='all')

def remove_empty_columns(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    """
    Remove columns with high percentage of null values.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        threshold (float): Threshold for null value percentage (default: 0.9)
        
    Returns:
        pd.DataFrame: DataFrame with empty columns removed
    """
    null_percent = df.isnull().sum() / len(df)
    cols_to_drop = null_percent[null_percent > threshold].index
    return df.drop(columns=cols_to_drop)

def standardize_dates(
    df: pd.DataFrame, 
    date_columns: List[str]
) -> pd.DataFrame:
    """
    Standardize date columns to datetime format.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        date_columns (List[str]): List of date column names
        
    Returns:
        pd.DataFrame: DataFrame with standardized date columns
    """
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def standardize_numeric_columns(
    df: pd.DataFrame, 
    numeric_columns: List[str]
) -> pd.DataFrame:
    """
    Standardize numeric columns by converting to float and handling special cases.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        numeric_columns (List[str]): List of numeric column names
        
    Returns:
        pd.DataFrame: DataFrame with standardized numeric columns
    """
    for col in numeric_columns:
        if col in df.columns:
            # Replace any non-numeric values with NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Replace negative values with 0 for quantity and value columns
            if any(x in col.upper() for x in ['QUANTITY', 'VALUE']):
                df[col] = df[col].clip(lower=0)
    return df

def handle_duplicate_values(
    df: pd.DataFrame,
    column: str,
    keep: str = 'first'
) -> pd.DataFrame:
    """
    Handle duplicate values in a specified column.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column (str): Column name to check for duplicates
        keep (str): Which duplicate to keep ('first', 'last', False)
        
    Returns:
        pd.DataFrame: DataFrame with handled duplicates
    """
    if df[column].duplicated().any():
        df = df.sort_values(by=[column])
        df[column] = df[column] + df.groupby(column).cumcount().astype(str)
        df[column] = df[column].str.replace('0', '')
    return df

def process_special_characters(
    df: pd.DataFrame,
    columns: List[str]
) -> pd.DataFrame:
    """
    Process special characters in specified columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        columns (List[str]): List of column names to process
        
    Returns:
        pd.DataFrame: DataFrame with processed special characters
    """
    for col in columns:
        if col in df.columns:
            # Replace tilde with separate rows
            if df[col].str.contains('~', na=False).any():
                df = df.assign(**{col: df[col].str.split('~')}).explode(col)
            
            # Clean up other special characters
            df[col] = df[col].str.strip()
            df[col] = df[col].str.replace('[^\w\s-]', '', regex=True)
    return df

def aggregate_data(
    df: pd.DataFrame,
    group_columns: List[str],
    agg_columns: Dict[str, str]
) -> pd.DataFrame:
    """
    Aggregate data based on specified columns and aggregation functions.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        group_columns (List[str]): Columns to group by
        agg_columns (Dict[str, str]): Dictionary of column names and aggregation functions
        
    Returns:
        pd.DataFrame: Aggregated DataFrame
    """
    return df.groupby(group_columns, as_index=False).agg(agg_columns)

def validate_mandatory_columns(
    df: pd.DataFrame,
    mandatory_columns: List[str]
) -> List[str]:
    """
    Validate presence of mandatory columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        mandatory_columns (List[str]): List of mandatory column names
        
    Returns:
        List[str]: List of missing mandatory columns
    """
    return [col for col in mandatory_columns if col not in df.columns]

def fill_missing_values(
    df: pd.DataFrame,
    fill_values: Dict[str, Any]
) -> pd.DataFrame:
    """
    Fill missing values in specified columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        fill_values (Dict[str, Any]): Dictionary of column names and fill values
        
    Returns:
        pd.DataFrame: DataFrame with filled missing values
    """
    for col, value in fill_values.items():
        if col in df.columns:
            df[col] = df[col].fillna(value)
    return df 