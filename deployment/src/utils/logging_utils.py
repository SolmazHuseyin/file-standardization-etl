"""
Logging utilities for the ETL job.
"""

import logging
import sys
from datetime import datetime

class ETLLogger:
    def __init__(self, job_name, log_level=logging.INFO):
        """
        Initialize the ETL logger.
        
        Args:
            job_name (str): Name of the ETL job
            log_level (int): Logging level (default: logging.INFO)
        """
        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(log_level)
        
        # Create formatters and handlers
        self._setup_handlers()
        
        self.job_name = job_name
        self.start_time = None
        self.end_time = None

    def _setup_handlers(self):
        """Set up console and file handlers with formatters."""
        # Create formatters
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_handler = logging.FileHandler(
            f'logs/etl_job_{timestamp}.log'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

    def start_job(self):
        """Log the start of the ETL job."""
        self.start_time = datetime.now()
        self.logger.info(f"Starting ETL job: {self.job_name}")
        self.logger.info(f"Start time: {self.start_time}")

    def end_job(self, status="completed"):
        """
        Log the end of the ETL job.
        
        Args:
            status (str): Job status (default: "completed")
        """
        self.end_time = datetime.now()
        duration = self.end_time - self.start_time if self.start_time else None
        
        self.logger.info(f"ETL job {status}: {self.job_name}")
        self.logger.info(f"End time: {self.end_time}")
        if duration:
            self.logger.info(f"Duration: {duration}")

    def log_step(self, step_name, status="started"):
        """
        Log an ETL step.
        
        Args:
            step_name (str): Name of the step
            status (str): Step status (default: "started")
        """
        self.logger.info(f"Step {status}: {step_name}")

    def log_error(self, error_msg, exc_info=None):
        """
        Log an error message.
        
        Args:
            error_msg (str): Error message
            exc_info (Exception, optional): Exception information
        """
        if exc_info:
            self.logger.error(error_msg, exc_info=exc_info)
        else:
            self.logger.error(error_msg)

    def log_warning(self, warning_msg):
        """
        Log a warning message.
        
        Args:
            warning_msg (str): Warning message
        """
        self.logger.warning(warning_msg)

    def log_info(self, info_msg):
        """
        Log an info message.
        
        Args:
            info_msg (str): Info message
        """
        self.logger.info(info_msg)

    def log_debug(self, debug_msg):
        """
        Log a debug message.
        
        Args:
            debug_msg (str): Debug message
        """
        self.logger.debug(debug_msg)

    def log_dataframe_info(self, df, df_name):
        """
        Log information about a DataFrame.
        
        Args:
            df: pandas or Spark DataFrame
            df_name (str): Name of the DataFrame
        """
        try:
            # Handle both pandas and Spark DataFrames
            if hasattr(df, 'shape'):  # pandas DataFrame
                rows, cols = df.shape
                self.logger.info(f"DataFrame {df_name}: {rows} rows, {cols} columns")
                self.logger.debug(f"Columns: {list(df.columns)}")
            else:  # Spark DataFrame
                rows = df.count()
                cols = len(df.columns)
                self.logger.info(f"DataFrame {df_name}: {rows} rows, {cols} columns")
                self.logger.debug(f"Columns: {df.columns}")
        except Exception as e:
            self.log_error(f"Error logging DataFrame info: {str(e)}")

    def log_column_mismatch(self, expected, actual):
        """Add detailed column mismatch logging"""
        self.log_error(
            f"Column mismatch found:\nExpected: {expected}\nActual: {actual}"
        )
