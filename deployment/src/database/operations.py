"""
Database operations module for handling specific database queries and operations.
"""

from config.queries import (
    DAQ_LOG_INFO_QUERY,
    ENTITY_DETAIL_QUERY_WITH_SHEET,
    ENTITY_DETAIL_QUERY_WITH_FILE,
    ATTRIBUTE_DETAIL_QUERY,
    UPDATE_DAQ_LOG_INFO,
    ENTITY_DETAIL_QUERY_NO_SHEET,
    ENTITY_DETAIL_QUERY_COUNTRY
)
from config.settings import DB_CONFIG

class DatabaseOperations:
    def __init__(self, db_connection):
        """
        Initialize database operations with a database connection.
        
        Args:
            db_connection: DatabaseConnection instance
        """
        self.db_connection = db_connection
        self.schema = DB_CONFIG['schema']

    def get_unprocessed_files(self):
        """
        Get unprocessed files from DAQ_LOG_INFO table.
        
        Returns:
            DataFrame: Spark DataFrame with unprocessed files information
        """
        params = {'schema': self.schema}
        return self.db_connection.execute_query(DAQ_LOG_INFO_QUERY, params)

    def get_entity_details_by_sheet(self, daq_sheet_name, sender_address):
        """
        Get entity details filtered by sheet name.
        
        Args:
            daq_sheet_name (str): Sheet name from DAQ log
            sender_address (str): Sender's email address
            
        Returns:
            DataFrame: Spark DataFrame with entity details
        """
        params = {
            'schema': self.schema,
            'daq_sheet_name': daq_sheet_name,
            'sender_address': sender_address
        }
        return self.db_connection.execute_query(ENTITY_DETAIL_QUERY_WITH_SHEET, params)

    def get_entity_details_by_file(self, daq_sheet_name, file_extension):
        """
        Get entity details filtered by file extension.
        
        Args:
            daq_sheet_name (str): Sheet name from DAQ log
            file_extension (str): File extension
            
        Returns:
            DataFrame: Spark DataFrame with entity details
        """
        params = {
            'schema': self.schema,
            'daq_sheet_name': daq_sheet_name,
            'file_extension': file_extension
        }
        return self.db_connection.execute_query(ENTITY_DETAIL_QUERY_WITH_FILE, params)

    def get_attribute_details(self, data_owner, context, file_table_name, sheet_name):
        """
        Get attribute details for a specific entity.
        
        Args:
            data_owner (str): Data owner name
            context (str): Context
            file_table_name (str): File/table name
            sheet_name (str): Sheet name
            
        Returns:
            DataFrame: Spark DataFrame with attribute details
        """
        params = {
            'schema': self.schema,
            'data_owner': data_owner,
            'context': context,
            'file_table_name': file_table_name,
            'sheet_name': sheet_name
        }
        return self.db_connection.execute_query(ATTRIBUTE_DETAIL_QUERY, params)

    def mark_file_as_processed(self, file_id=None):
        """
        Mark file(s) as processed in DAQ_LOG_INFO table.
        
        Args:
            file_id (int, optional): Specific file ID to mark as processed.
                                   If None, marks all unprocessed files.
        """
        condition = f"id = {file_id}" if file_id else "is_corrupt = 0 and is_processed = 0"
        params = {
            'schema': self.schema,
            'condition': condition
        }
        self.db_connection.execute_update(UPDATE_DAQ_LOG_INFO, params)

    def insert_temp_load_info(self, table_name, context, frequency, is_invoice, is_process):
        """
        Insert record into temp_load_info table.
        
        Args:
            table_name (str): Table name
            context (str): Context
            frequency (str): Frequency
            is_invoice (int): Invoice flag
            is_process (int): Process flag
        """
        query = f"""
        INSERT INTO {self.schema}.temp_load_info 
        (table_name, context, frequency, is_invoice, is_process, load_datetime)
        VALUES 
        ('{table_name.upper()}', '{context}', '{frequency}', {is_invoice}, {is_process}, CURRENT_TIMESTAMP)
        """
        self.db_connection.execute_update(query)

    def get_entity_details(self, daq_sheet_name, sender_address, file_extension, receiver_address, file_id):
        """
        Get entity details using sequential matching strategy.
        Tries different matching criteria in sequence until a match is found.
        
        Args:
            daq_sheet_name (str): Sheet name from DAQ log
            sender_address (str): Sender's email address
            file_extension (str): File extension
            receiver_address (str): Receiver's email address
            file_id (int): File ID from DAQ_LOG_INFO
            
        Returns:
            DataFrame: Spark DataFrame with entity details from the first successful match
        """
        # Step 1: Try sheet name match first (strictest match)
        result = self.get_entity_details_by_sheet(daq_sheet_name, sender_address)
        if result.count() > 0:
            return result
            
        # Step 2: Try file extension match
        result = self.get_entity_details_by_file(daq_sheet_name, file_extension)
        if result.count() > 0:
            return result
            
        # Step 3: Try without sheet name restriction
        result = self.get_entity_details_no_sheet(daq_sheet_name, sender_address)
        if result.count() > 0:
            return result
            
        # Step 4: Finally try country and filename matching (most relaxed criteria)
        return self.get_entity_details_by_country(
            daq_sheet_name, file_extension, sender_address, receiver_address, file_id
        )

    def get_entity_details_no_sheet(self, daq_sheet_name, sender_address):
        """Get entity details without sheet name filter"""
        params = {
            'schema': self.schema,
            'daq_sheet_name': daq_sheet_name,
            'sender_address': sender_address
        }
        return self.db_connection.execute_query(ENTITY_DETAIL_QUERY_NO_SHEET, params)

    def get_entity_details_by_country(self, daq_sheet_name, file_extension, sender_address, receiver_address, file_id):
        """Get entity details with country and filename matching"""
        params = {
            'schema': self.schema,
            'daq_sheet_name': daq_sheet_name,
            'file_extension': file_extension,
            'sender_address': sender_address,
            'receiver_address': receiver_address,
            'file_id': file_id
        }
        return self.db_connection.execute_query(ENTITY_DETAIL_QUERY_COUNTRY, params)
