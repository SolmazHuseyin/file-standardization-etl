"""
Data loading module for the ETL job.
"""

from datetime import datetime
from src.models.stock import StockData
from src.models.sales import SalesData

class DataLoader:
    def __init__(self, db_connection, logger):
        """
        Initialize the data loader.
        
        Args:
            db_connection: Database connection object
            logger: Logger instance
        """
        self.db = db_connection
        self.logger = logger

    def load_stock_data(self, stock_data, table_name):
        """
        Load stock data into the database.
        
        Args:
            stock_data (StockData): Stock data to load
            table_name (str): Target table name
            
        Returns:
            bool: True if loading successful, False otherwise
        """
        if not isinstance(stock_data, StockData):
            self.logger.log_error("Invalid stock data type")
            return False
            
        try:
            # Convert to Spark DataFrame
            spark = self.db.get_spark_session()
            spark_df = stock_data.to_spark_df(spark)
            
            # Write to database
            (spark_df.write
                .format("jdbc")
                .option("url", self.db.jdbc_url)
                .option("dbtable", table_name)
                .option("user", self.db.jdbc_properties['user'])
                .option("password", self.db.jdbc_properties['password'])
                .option("driver", self.db.jdbc_properties['driver'])
                .mode("append")
                .save())
            
            self.logger.log_info(f"Successfully loaded stock data to table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.log_error(f"Error loading stock data: {str(e)}", exc_info=e)
            return False

    def load_sales_data(self, sales_data, table_name):
        """
        Load sales data into the database.
        
        Args:
            sales_data (SalesData): Sales data to load
            table_name (str): Target table name
            
        Returns:
            bool: True if loading successful, False otherwise
        """
        if not isinstance(sales_data, SalesData):
            self.logger.log_error("Invalid sales data type")
            return False
            
        try:
            # Convert to Spark DataFrame
            spark = self.db.get_spark_session()
            spark_df = sales_data.to_spark_df(spark)
            
            # Write to database
            (spark_df.write
                .format("jdbc")
                .option("url", self.db.jdbc_url)
                .option("dbtable", table_name)
                .option("user", self.db.jdbc_properties['user'])
                .option("password", self.db.jdbc_properties['password'])
                .option("driver", self.db.jdbc_properties['driver'])
                .mode("append")
                .save())
            
            self.logger.log_info(f"Successfully loaded sales data to table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.log_error(f"Error loading sales data: {str(e)}", exc_info=e)
            return False

    def create_temp_table(self, table_name, schema):
        """
        Create a temporary table for data loading.
        
        Args:
            table_name (str): Table name
            schema (dict): Column schema dictionary
            
        Returns:
            bool: True if creation successful, False otherwise
        """
        try:
            # Generate CREATE TABLE statement
            columns = []
            for col_name, col_type in schema.items():
                sql_type = self._map_pandas_to_sql_type(col_type)
                columns.append(f"{col_name} {sql_type}")
            
            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)},
                LOAD_DATETIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # Execute creation
            self.db.execute_update(create_stmt)
            self.logger.log_info(f"Successfully created temporary table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.log_error(f"Error creating temporary table: {str(e)}", exc_info=e)
            return False

    def _map_pandas_to_sql_type(self, pandas_type):
        """
        Map pandas dtype to SQL type.
        
        Args:
            pandas_type (str): pandas dtype
            
        Returns:
            str: Corresponding SQL type
        """
        type_mapping = {
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'TEXT',
            'Int64': 'BIGINT',
            'float64': 'DECIMAL(22,2)',
            'bool': 'BOOLEAN'
        }
        return type_mapping.get(pandas_type, 'TEXT')

    def cleanup_temp_tables(self, retention_days=7):
        """
        Clean up old temporary tables.
        
        Args:
            retention_days (int): Number of days to retain tables
            
        Returns:
            bool: True if cleanup successful, False otherwise
        """
        try:
            cleanup_query = f"""
            DELETE FROM temp_load_info
            WHERE load_datetime < CURRENT_TIMESTAMP - INTERVAL '{retention_days} days'
            """
            
            self.db.execute_update(cleanup_query)
            self.logger.log_info(f"Successfully cleaned up temporary tables older than {retention_days} days")
            return True
            
        except Exception as e:
            self.logger.log_error(f"Error cleaning up temporary tables: {str(e)}", exc_info=e)
            return False

    def update_processing_status(self, file_id):
        """Add comprehensive status update"""
        try:
            self.db.execute_update(
                "UPDATE daq_log_info SET is_processed = 1 WHERE id = %s",
                (file_id,)
            )
            self.db.commit()
        except Exception as e:
            self.logger.log_error(f"Status update failed: {str(e)}")
            self.db.rollback()
