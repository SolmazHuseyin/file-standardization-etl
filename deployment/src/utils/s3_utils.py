"""
S3 utility functions for file operations.
"""

import pandas as pd
import s3fs
from typing import Union, Optional

class S3Manager:
    """Utility class for S3 operations."""
    
    def __init__(self, region_name: Optional[str] = None):
        """
        Initialize S3 manager.
        
        Args:
            region_name (str, optional): AWS region name
        """
        self.s3 = s3fs.S3FileSystem(anon=False)
        if region_name:
            self.s3.region_name = region_name
    
    def get_file(
        self, 
        s3_path: str,
        file_type: str = 'excel'
    ) -> pd.DataFrame:
        """
        Get file from S3.
        
        Args:
            s3_path (str): S3 path to the file
            file_type (str): Type of file ('excel' or 'parquet')
            
        Returns:
            pd.DataFrame: DataFrame containing file contents
        """
        with self.s3.open(s3_path, 'rb') as f:
            if file_type.lower() == 'excel':
                return pd.read_excel(f, engine='openpyxl')
            elif file_type.lower() == 'parquet':
                return pd.read_parquet(f)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
    
    def save_file(
        self, 
        data: pd.DataFrame,
        s3_path: str,
        file_type: str = 'parquet'
    ) -> None:
        """
        Save processed data back to S3.
        
        Args:
            data (pd.DataFrame): Data to save
            s3_path (str): S3 path to save to
            file_type (str): Type of file to save ('parquet' or 'excel')
        """
        with self.s3.open(s3_path, 'wb') as f:
            if file_type.lower() == 'parquet':
                data.to_parquet(f)
            elif file_type.lower() == 'excel':
                data.to_excel(f, engine='openpyxl', index=False)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
    
    def list_files(
        self, 
        s3_path: str,
        pattern: Optional[str] = None
    ) -> list:
        """
        List files in S3 path.
        
        Args:
            s3_path (str): S3 path to list
            pattern (str, optional): File pattern to match
            
        Returns:
            list: List of file paths
        """
        if pattern:
            return self.s3.glob(f"{s3_path}/{pattern}")
        return self.s3.ls(s3_path) 