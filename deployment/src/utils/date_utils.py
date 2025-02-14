"""
Date handling utilities for the ETL job.
"""

from datetime import datetime, timedelta
import pandas as pd
from config.settings import DATE_FORMATS
from typing import Optional

def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string using multiple formats.
    
    Args:
        date_str (str): Date string to parse
        
    Returns:
        datetime: Parsed datetime object or None if parsing fails
    """
    if not date_str or pd.isna(date_str):
        return None
        
    date_str = str(date_str).strip()
    
    # Try pandas to_datetime first
    try:
        return pd.to_datetime(date_str)
    except:
        pass
    
    # Try all defined formats
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except:
            continue
            
    return None

def format_date(date: datetime, fmt: str = "%Y-%m-%d") -> str:
    """
    Format datetime object to string.
    
    Args:
        date (datetime): Date to format
        fmt (str): Output format
        
    Returns:
        str: Formatted date string
    """
    if not date:
        return None
    return date.strftime(fmt)

def get_last_day_of_month(date: datetime) -> datetime:
    """
    Get the last day of the month for a given date.
    
    Args:
        date (datetime): Input date
        
    Returns:
        datetime: Last day of the month
    """
    if date.month == 12:
        return date.replace(day=31)
    next_month = date.replace(day=1, month=date.month + 1)
    return next_month - timedelta(days=1)

def validate_date_range(date: datetime, min_date: Optional[datetime] = None, 
                       max_date: Optional[datetime] = None) -> bool:
    """
    Validate if date is within specified range.
    
    Args:
        date (datetime): Date to validate
        min_date (datetime, optional): Minimum allowed date
        max_date (datetime, optional): Maximum allowed date
        
    Returns:
        bool: True if date is within range
    """
    if not date:
        return False
        
    if min_date and date < min_date:
        return False
        
    if max_date and date > max_date:
        return False
        
    return True

def get_date_parts(date_obj):
    """
    Extract year, month, and day from a date object.
    
    Args:
        date_obj (datetime): Date to extract parts from
        
    Returns:
        tuple: (year, month, day) or None if extraction fails
    """
    if not date_obj:
        return None
        
    try:
        return date_obj.year, date_obj.month, date_obj.day
    except AttributeError:
        return None
