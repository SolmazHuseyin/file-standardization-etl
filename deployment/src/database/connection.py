"""
Database connection handling module.
"""

import json
import boto3
from pyspark.sql import SparkSession
from config.settings import DB_CONFIG, AWS_REGION

class DatabaseConnection:
    def __init__(self, secret_name):
        """
        Initialize database connection with AWS Secrets Manager credentials.
        
        Args:
            secret_name (str): Name of the secret in AWS Secrets Manager
        """
        self.secret_name = secret_name
        self.credentials = self._get_secret()
        self.jdbc_url = f"jdbc:postgresql://{self.credentials['host']}:{self.credentials['port']}/{self.credentials['dbname']}"
        self.jdbc_properties = {
            'user': self.credentials['username'],
            'password': self.credentials['password'],
            'driver': DB_CONFIG['driver']
        }

    def _get_secret(self):
        """
        Retrieve database credentials from AWS Secrets Manager.
        
        Returns:
            dict: Database credentials
        """
        client = boto3.client('secretsmanager', region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=self.secret_name)
        return json.loads(response['SecretString'])

    def get_spark_session(self):
        """
        Get or create a Spark session.
        
        Returns:
            SparkSession: Active Spark session
        """
        return SparkSession.builder.appName("PostgreSQL Connection").getOrCreate()

    def execute_query(self, query, params=None):
        """
        Execute a SQL query using Spark JDBC connection.
        
        Args:
            query (str): SQL query to execute
            params (dict, optional): Parameters to format the query with
        
        Returns:
            DataFrame: Spark DataFrame with query results
        """
        if params:
            query = query.format(**params)

        spark = self.get_spark_session()
        return (spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("query", query)
                .option("driver", DB_CONFIG['driver'])
                .option("user", self.credentials['username'])
                .option("password", self.credentials['password'])
                .load())

    def execute_update(self, query, params=None):
        """
        Execute an update SQL query using JDBC connection.
        
        Args:
            query (str): SQL update query to execute
            params (dict, optional): Parameters to format the query with
        """
        if params:
            query = query.format(**params)

        spark = self.get_spark_session()
        conn = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(
            self.jdbc_url,
            self.credentials['username'],
            self.credentials['password']
        )
        
        try:
            stmt = conn.createStatement()
            stmt.execute(query)
        finally:
            if conn:
                conn.close()
