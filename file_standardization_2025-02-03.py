# Standard Python libraries
import datetime
from datetime import date, timedelta, datetime
import calendar
import json
import re
from decimal import Decimal
import sys

# Third-party libraries
import boto3
import s3fs
import pandas as pd
import numpy as np

# PySpark and AWS Glue libraries
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DateType,
    LongType,
    DecimalType,
)
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ============================================================
# Job Initialization
# ============================================================
args = getResolvedOptions(sys.argv, [JOB_NAME, secret_name, env])

sparkContext = SparkContext()
glueContext = GlueContext(sparkContext)
sparkSession = glueContext.spark_session

glueJob = Job(glueContext)
glueJob.init(args[JOB_NAME], args)


# AWS Secrets Manager
def get_secret(secret_name):
    client = boto3.client('secretsmanager', region_name='eu-central-1')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(get_secret_value_response[SecretString])
    return secret


secret_name = args[secret_name]
secret = get_secret(secret_name)
username = secret[username]
password = secret[password]
schemaname = imip
host = secret[host]
port = secret[port]
dbname = secret[dbname]

jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"

jdbc_properties = {
    "user": username,
    "password": password,
    "driver": "org.postgresql.Driver",
}

# ============================================================
# Define the path where the files are located as s3_main_path
# ============================================================

env_lower = args['env'].lower()
print(f"Debug 230: The environment is set to {env_lower}")

s3_main_path = f"s3://pt-s3-imip-{env_lower}-imip-all-data/mail/mntc/RocheIMIP-file/RECEIVED_FILE_PATH/"

# ============================================================
# Function to Insert Data into temp_load_info Table
# ============================================================


def insert_into_temp_load_info(v_table_name, v_context, v_frequency, v_is_invoice, v_is_process):
    v_table_name = v_table_name.upper()
    # Convert v_table_name to uppercase

    # Define the schema for the temp_load_info table
    print("Step 1: Defining schema for temp_load_info table.")

    schema = StructType(
        [
            StructField(table_name, StringType(), nullable=True),
            StructField(context, StringType(), nullable=True),
            StructField(frequency, StringType(), nullable=True),
            StructField(is_invoice, IntegerType(), nullable=True),
            StructField(is_process, IntegerType(), nullable=True),
            StructField(load_datetime, TimestampType(), nullable=True),
        ]
    )

    print("Step 2: Creating DataFrame with the provided values.")
    spark = SparkSession.builder.appName("PostgreSQL Writing").getOrCreate()
    print("Step 3: Creating Spark session for PostgreSQL write.")
    load_datetime = datetime.now()

    data = [
        Row(
            table_name=v_table_name,
            context=v_context,
            frequency=v_frequency,
            is_invoice=v_is_invoice,
            is_process=v_is_process,
            load_datetime=load_datetime,
        )
    ]
    df = spark.createDataFrame(data, schema=schema)

    print("Step 4: Writing DataFrame to PostgreSQL database.")
    df.write.format(jdbc).option(url, jdbc_url).option(dbtable, f"{schemaname}.temp_load_info").option(
        user, username
    ).option(password, password).mode(append).save()

    print("Step 5: Data successfully written to temp_load_info table.")


# ============================================================
# Function to update_daq_log_info
# ============================================================


def update_daq_log_info(id):
    
    """
    Updates the daq_log_info table in PostgreSQL to mark a record as processed.


    Parameters
    - id The ID of the record to be updated (can be numeric or non-numeric).
    - schemaname The schema name where the table is located.
    - jdbc_url JDBC URL for the PostgreSQL database.
    - username Username for the database connection.
    - password Password for the database connection.
    """

    # Create PostgreSQL JDBC connection
    spark = SparkSession.builder.appName("PostgreSQL Writing").getOrCreate()
    connSpark = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
    stmt = connSpark.createStatement()
    # Check if id is numeric
    if isinstance(id, (int, float)):

        sql_execute_query = (
            f"UPDATE {schemaname}.daq_log_info SET is_processed = 1 WHERE id = {id}"
        )
    else:
        sql_execute_query = f"UPDATE {schemaname}.daq_log_info SET is_processed = 1 WHERE is_corrupt = 0 and is_processed = 0"


    print(f"Executing SQL update, {sql_execute_query}")
    stmt.execute(sql_execute_query)
    print("DAQ_LOG_INFO updated successfully.")
    if connSpark:
        connSpark.close()



# ============================================================
# Data is Selected from DAQ_LOG_INFO Table
# ============================================================

query_daq_log_info = f"""
    SELECT DISTINCT id, receiver_address, sender_address, file, sheet_name as daq_sheet_name, mail_date, 
    CASE 
        WHEN position('.' in reverse(file)) > 0 THEN 
        substring(file from (char_length(file) - position('.' in reverse(file)) + 2)) 
    ELSE 'EMPTY' 
    END as file_extension, 
    (date_trunc('month', mail_date) - INTERVAL '1 day') AS mail_date_prev_month_last_day 
    FROM {schemaname}.daq_log_info a 
    WHERE EXISTS ( 
        SELECT 1 
        FROM ( 
            SELECT 
                file, 
                sheet_name, 
                subject, 
                receiver_address, 
                sender_address, 
                to_char(mail_date, 'yyyymmdd') as str_mail_date, 
                max(id) as max_id 
            FROM {schemaname}.daq_log_info 
            WHERE is_corrupt = 0 AND is_processed = 0 
            GROUP BY file, sheet_name, subject, receiver_address, sender_address, to_char(mail_date, 'yyyymmdd') 
        ) b 
        WHERE a.id = b.max_id 
    )
"""

source_df = (
    sparkSession.read.format(jdbc)
    .option(url, jdbc_url)
    .option(query, query_daq_log_info)
    .option(driver, org.postgresql.Driver)
    .option(user, username)
    .option(password, password)
    .load()
)

dynamic_dframe = DynamicFrame.fromDF(source_df, glueContext, dynamic_df)

result_daq_log_info = (
    dynamic_dframe.select_fields(
        [
            id,
            receiver_address,
            sender_address,
            file,
            daq_sheet_name,
            mail_date,
            file_extension,
            mail_date_prev_month_last_day,
        ]
    )
    .toDF()
    .collect()
)

# ============================================================
# Data is Selected from DAQ_LOG_INFO Table and For loop is Started
# ============================================================

for row_daq_log_info in result_daq_log_info:
    (
        id,
        receiver_address,
        sender_address,
        file,
        daq_sheet_name,

        mail_date,
        file_extension,
        mail_date_prev_month_last_day,
    ) = row_daq_log_info
    table_name = f"temp_{id}"
    print(id , id)
    print(receiver_address , receiver_address)
    print(sender_address , sender_address)
    print(file , file)
    print(daq_sheet_name , daq_sheet_name)
    print(mail_date , mail_date)
    print(file_extension , file_extension)
    print(mail_date_prev_month_last_day , mail_date_prev_month_last_day)
    print("--------------------------------")


    # ============================================================
    # Read data from DD_ENTITY_DETAIL table
    # With Filter Applied to Sheet_name Column
    # Filter is applied to data_owner_mail and sender_address columns
    # ============================================================

    query_entity_detail = (f"""
        select distinct data_owner, country, context, period, frequency, data_owner_mail, structure, 
               file_table_name as entity_file_table_name, sheet_name as entity_sheet_name, 
               lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1, 
               lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) as sheet_name2, 
               case when lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
                         lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) 
                    then 1 
                    else 0 
               end as sheet_name_comparison_result 
          from  + schemaname + .dd_entity_detail 
        WHERE is_api = 0 
        AND data_owner_mail = ' + sender_address + ' 
        AND lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) 
    """)
    print(f"query_entity_detail 1 , {query_entity_detail}")



    source_df_entity_detail = (
        sparkSession.read.format(jdbc)
        .option(url, jdbc_url)
        .option(query, query_entity_detail)
        .option(driver, org.postgresql.Driver)
        .option(user, username)
        .option(password, password)
        .load()
    )

    dynamic_dframe_entity_detail = DynamicFrame.fromDF(source_df_entity_detail, glueContext, dynamic_df)

    result_entity_detail = (
        dynamic_dframe_entity_detail.select_fields(
            [
                data_owner,
                country,
                context,
                period,
                frequency,
                data_owner_mail,
                structure,
                entity_file_table_name,
                entity_sheet_name,
                sheet_name1,
                sheet_name2,
                sheet_name_comparison_result,
            ]
        )
        .toDF()
        .collect()
    )

    # ============================================================
    # Read data from DD_ENTITY_DETAIL table
    # With Filter Applied to Sheet_name Column
    # Filter is applied on File Name and COUNTRY Columns in DAQ_LOG_INFO Table
    # If there is no data from the first query, run the second query.
    # ============================================================

    if len(result_entity_detail) == 0:
        query_entity_detail = (f"""
            SELECT DISTINCT data_owner, country, context, period, frequency, data_owner_mail, structure, 
                   file_table_name as entity_file_table_name, sheet_name as entity_sheet_name, 

                   lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1, 
                   lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) as sheet_name2, 
                   case when lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
                   lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) then 1 else 0 end as sheet_name_comparison_result 
              FROM 
                   (SELECT ed., 
                           case  
                                when position('.' in reverse(file_table_name))  0 
                                then substring(file_table_name from (char_length(file_table_name) - position('.' in reverse(file_table_name)) + 2)) 
                                else 'EMPTY' 
                            end as file_extension 
                      FROM  + schemaname + .dd_entity_detail ed 
                   ) 
             WHERE is_api = 0 
               AND 
                   CASE 
                        WHEN upper(coalesce(replace(file_extension, 'n', ''), 'file_ext')) = 'XLS' 
                        THEN 'XLSX' 
                        ELSE upper(coalesce(replace(file_extension, 'n', ''), 'file_ext')) 
                    END = 
                   CASE 
                        WHEN replace(upper(coalesce(replace(' + file_extension + ', 'n', ''), 'file_ext')), 'İ', 'I') = 'XLS' 
                        THEN 'XLSX' 
                        ELSE replace(upper(coalesce(replace(' + file_extension + ', 'n', ''), 'file_ext')), 'İ', 'I') 
                    END 
            AND sheet_name = ' + daq_sheet_name + ' 
            AND lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) 
             AND ( 
                  ( 
                    (replace(replace(Upper(data_owner), 'İ', 'I'), ' ', '') = replace(replace(Upper(' + sender_address + '), 'İ', 'I'), ' ', '')) 
                    OR 
                    ((SELECT (Regexp_matches(data_owner_mail, '@([^.]+)'))[1]) = ' + sender_address + ') 
                  ) 
                  OR 
                  ( 
                    country = ( 
                              SELECT DISTINCT dc.country_name 
                                FROM  + schemaname + .email_connection_info eci, 
                                      + schemaname + .dim_countries dc 
                               WHERE eci.email_address = ' + receiver_address + ' 
                                 AND dc.country_id = eci.country_id 
                               ) 
                    AND (CASE 
                              WHEN POSITION('-' IN file_table_name)  0 
                              THEN SUBSTRING(file_table_name, 1, POSITION('-' IN file_table_name) - 1) 
                              WHEN POSITION('.' IN file_table_name)  0 
                              THEN SUBSTRING(file_table_name, 1, POSITION('.' IN file_table_name) - 1) 
                              ELSE file_table_name 
                          END) = 
                        (SELECT DISTINCT 
                                CASE 
                                     WHEN POSITION('-' IN file)  0 THEN SUBSTRING(file, 1, POSITION('-' IN file) - 1) 
                                     WHEN POSITION('.' IN file)  0 THEN SUBSTRING(file, 1, POSITION('.' IN file) - 1) 
                                     ELSE file 
                                 END AS file 
                            FROM  + schemaname + .daq_log_info 
                           WHERE id =  + str(id) + ) 
                        ) 
            ) 
        """)
        print(f"query_entity_detail 2 , {query_entity_detail}")


        source_df_entity_detail = (
            sparkSession.read.format(jdbc)
            .option(url, jdbc_url)
            .option(query, query_entity_detail)
            .option(driver, org.postgresql.Driver)
            .option(user, username)
            .option(password, password)
            .load()
        )

        dynamic_dframe_entity_detail = DynamicFrame.fromDF(source_df_entity_detail, glueContext, dynamic_df)

        result_entity_detail = (
            dynamic_dframe_entity_detail.select_fields(
                [
                    data_owner,
                    country,
                    context,
                    period,
                    frequency,
                    data_owner_mail,
                    structure,
                    entity_file_table_name,
                    entity_sheet_name,
                    sheet_name1,
                    sheet_name2,
                    sheet_name_comparison_result,
                ]
            )
            .toDF()
            .collect()
        )

    # ============================================================
    # Read data from DD_ENTITY_DETAIL table
    # Without Filter Applied to Sheet_name Column
    # Filter is applied to data_owner_mail and sender_address columns
    # If there is no data from the third query, run the second query.
    # ============================================================

    if len(result_entity_detail) == 0:
        query_entity_detail = (f"""
            select distinct data_owner, country, context, period, frequency, data_owner_mail, structure, 
                   file_table_name as entity_file_table_name, sheet_name as entity_sheet_name, 

                   lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1, 
                   lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) as sheet_name2, 
                   case when lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
                             lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) 
                        then 1 
                        else 0 
                   end as sheet_name_comparison_result 
              from  + schemaname + .dd_entity_detail 
             WHERE is_api = 0 
               AND data_owner_mail = ' + sender_address + ' 
        """)
        print(f"query_entity_detail 3 , {query_entity_detail}")


        source_df_entity_detail = (
            sparkSession.read.format(jdbc)
            .option(url, jdbc_url)
            .option(query, query_entity_detail)
            .option(driver, org.postgresql.Driver)
            .option(user, username)
            .option(password, password)
            .load()
        )

        dynamic_dframe_entity_detail = DynamicFrame.fromDF(source_df_entity_detail, glueContext, dynamic_df)

        result_entity_detail = (
            dynamic_dframe_entity_detail.select_fields(
                [
                    data_owner,
                    country,
                    context,
                    period,
                    frequency,
                    data_owner_mail,
                    structure,
                    entity_file_table_name,
                    entity_sheet_name,
                    sheet_name1,
                    sheet_name2,
                    sheet_name_comparison_result,
                ]
            )
            .toDF()
            .collect()
        )

    # ============================================================
    # Read data from DD_ENTITY_DETAIL table
    # Without Filter Applied to Sheet_name Column
    # Filter is applied on File Name and COUNTRY Columns in DAQ_LOG_INFO Table
    # If there is no data from the fourth query, run the second query.
    # ============================================================

    if len(result_entity_detail) == 0:
        query_entity_detail = (f"""
            SELECT DISTINCT data_owner, country, context, period, frequency, data_owner_mail, structure, 
                   file_table_name as entity_file_table_name, sheet_name as entity_sheet_name, 

                   lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1, 
                   lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) as sheet_name2, 
                   case when lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
                   lower(regexp_replace(' + daq_sheet_name + ', '[0-9]', '', 'g')) then 1 else 0 end as sheet_name_comparison_result 
              FROM 
                   (SELECT ed., 
                           case  
                                when position('.' in reverse(file_table_name))  0 
                                then substring(file_table_name from (char_length(file_table_name) - position('.' in reverse(file_table_name)) + 2)) 
                                else 'EMPTY' 
                            end as file_extension 
                      FROM  + schemaname + .dd_entity_detail ed 
                   ) 
             WHERE is_api = 0 
               AND 
                   CASE 
                        WHEN upper(coalesce(replace(file_extension, 'n', ''), 'file_ext')) = 'XLS' 
                        THEN 'XLSX' 
                        ELSE upper(coalesce(replace(file_extension, 'n', ''), 'file_ext')) 
                    END = 
                   CASE 
                        WHEN replace(upper(coalesce(replace(' + file_extension + ', 'n', ''), 'file_ext')), 'İ', 'I') = 'XLS' 
                        THEN 'XLSX' 
                        ELSE replace(upper(coalesce(replace(' + file_extension + ', 'n', ''), 'file_ext')), 'İ', 'I') 
                    END 
             AND ( 
                  ( 
                    (replace(replace(Upper(data_owner), 'İ', 'I'), ' ', '') = replace(replace(Upper(' + sender_address + '), 'İ', 'I'), ' ', '')) 
                    OR 
                    ((SELECT (Regexp_matches(data_owner_mail, '@([^.]+)'))[1]) = ' + sender_address + ') 
                  ) 
                  OR 
                  ( 
                    country = ( 
                              SELECT DISTINCT dc.country_name 
                                FROM  + schemaname + .email_connection_info eci, 
                                      + schemaname + .dim_countries dc 
                               WHERE eci.email_address = ' + receiver_address + ' 
                                 AND dc.country_id = eci.country_id 
                               ) 
                    AND (CASE 
                              WHEN POSITION('-' IN file_table_name)  0 
                              THEN SUBSTRING(file_table_name, 1, POSITION('-' IN file_table_name) - 1) 
                              WHEN POSITION('.' IN file_table_name)  0 
                              THEN SUBSTRING(file_table_name, 1, POSITION('.' IN file_table_name) - 1) 
                              ELSE file_table_name 
                          END) = 
                        (SELECT DISTINCT 
                                CASE 
                                     WHEN POSITION('-' IN file)  0 THEN SUBSTRING(file, 1, POSITION('-' IN file) - 1) 
                                     WHEN POSITION('.' IN file)  0 THEN SUBSTRING(file, 1, POSITION('.' IN file) - 1) 
                                     ELSE file 
                                 END AS file 
                            FROM  + schemaname + .daq_log_info 
                           WHERE id =  + str(id) + ) 
                        ) 
            ) 
        )""")
        print(f"query_entity_detail 4 , {query_entity_detail}")



        source_df_entity_detail = (
            sparkSession.read.format(jdbc)
            .option(url, jdbc_url)
            .option(query, query_entity_detail)
            .option(driver, org.postgresql.Driver)
            .option(user, username)
            .option(password, password)
            .load()
        )

        dynamic_dframe_entity_detail = DynamicFrame.fromDF(source_df_entity_detail, glueContext, dynamic_df)

        result_entity_detail = (
            dynamic_dframe_entity_detail.select_fields(
                [
                    data_owner,
                    country,
                    context,
                    period,
                    frequency,
                    data_owner_mail,
                    structure,
                    entity_file_table_name,
                    entity_sheet_name,
                    sheet_name1,
                    sheet_name2,
                    sheet_name_comparison_result,
                ]
            )
            .toDF()
            .collect()
        )

    # ============================================================
    # Read data from table DD_ATTRIBUTE_DETAIL in the loop
    # ============================================================

    for row_entity_detail in result_entity_detail:

        (
            data_owner,
            country,
            context,
            period,
            frequency,
            data_owner_mail,
            structure,
            entity_file_table_name,
            entity_sheet_name,
            sheet_name1,
            sheet_name2,
            sheet_name_comparison_result,
        ) = row_entity_detail
        print(data_owner , data_owner)
        print(country , country)
        print(context , context)
        print(period , period)
        print(frequency , frequency)
        print(data_owner_mail , data_owner_mail)
        print(structure , structure)
        print(entity_file_table_name , entity_file_table_name)
        print(entity_sheet_name , entity_sheet_name)
        print(sheet_name1 , sheet_name1)
        print(sheet_name2 , sheet_name2)
        print(sheet_name_comparison_result , sheet_name_comparison_result)

        if data_owner == ALLIANCE and country == TURKIYE and daq_sheet_name.lower() != urundepobazinda:
            print(f"Condition met Data owner is '{data_owner}' and entity sheet name '{daq_sheet_name}' is different from 'urundepobazinda'.")
            print(f"Condition met Exiting the loop for '{daq_sheet_name}'.")
            break


        if data_owner == RAFED and country == UAE and daq_sheet_name.lower() not in ("mafraq-ssmc data", "tawam data"):
            print(f"Condition met Data owner is '{data_owner}' and entity sheet name '{daq_sheet_name}' is different from 'urundepobazinda'.")
            print(f"Condition met Exiting the loop for '{daq_sheet_name}'.")
            break


        query_attribute_detail = (f"""
            select original_column_name, second_column_name, etl_column_name, 
             column_position, starting_row, is_mandatory 

            from  + schemaname + .dd_attribute_detail 
            where data_owner = ' + data_owner + ' 
            and context = ' + context + ' 
            and file_table_name = ' + entity_file_table_name + ' 
            and sheet_name = ' + entity_sheet_name + ' 
            order by column_position, starting_row
        """)
        print(f"query_attribute_detail , {query_attribute_detail}")

        source_df_attribute_detail = (
            sparkSession.read.format(jdbc)

            .option(url, jdbc_url)
            .option(query, query_attribute_detail)
            .option(driver, org.postgresql.Driver)
            .option(user, username)
            .option(password, password)
            .load()
        )

        dynamic_dframe_attribute_detail = DynamicFrame.fromDF(source_df_attribute_detail, glueContext, dynamic_df)

        result_attribute_detail = (
            dynamic_dframe_attribute_detail.select_fields(
                [
                    original_column_name,
                    second_column_name,
                    etl_column_name,
                    column_position,
                    starting_row,
                    is_mandatory,
                ]
            )
            .toDF()
            .collect()
        )

        # ============================================================
        # Creating DataFrame for Inventory
        # ============================================================

        df_Result_Stock = pd.DataFrame(
            {
                "DATA_DATE": pd.Series(dtype=datetime64[ns]),  # DATE
                "COUNTRY_NAME": pd.Series(dtype=object),  # TEXT
                "ORGANIZATION_NAME": pd.Series(dtype=object),  # TEXT
                "BRANCH_NAME": pd.Series(dtype=object),  # TEXT
                "PRODUCT_ID": pd.Series(dtype=object),  # TEXT
                "PRODUCT_NAME": pd.Series(dtype=object),  # TEXT

                "AVAILABLE_QUANTITY": pd.Series(dtype=Int64),  # Nullable bigint
                "BLOCKED_QUANTITY": pd.Series(dtype=Int64),  # Nullable bigint
                "INVENTORY_CATEGORY": pd.Series(dtype=object),  # TEXT
                "BATCH_NUMBER": pd.Series(dtype=object),  # TEXT
                "EXPIRY_DATE": pd.Series(dtype=datetime64[ns]),  # DATE
            }
        )


        # ============================================================
        # Creating DataFrame for Sales
        # ============================================================

        df_Result_Sales = pd.DataFrame(
            {
                "DATA_DATE": pd.Series(dtype=datetime64[ns]),  # DATE
                "COUNTRY_NAME": pd.Series(dtype=object),  # TEXT
                "ORGANIZATION_NAME": pd.Series(dtype=object),  # TEXT
                "BRANCH_NAME": pd.Series(dtype=object),  # TEXT
                "CUSTOMER_ID": pd.Series(dtype=object),  # TEXT
                "CUSTOMER_NAME": pd.Series(dtype=object),  # TEXT

                "PRODUCT_ID": pd.Series(dtype=object),  # TEXT
                "PRODUCT_NAME": pd.Series(dtype=object),  # TEXT
                "INVOICE_DATE": pd.Series(dtype=datetime64[ns]),  # DATE
                "SALES_QUANTITY": pd.Series(dtype=Int64),  # BIGINT
                "RETURN_QUANTITY": pd.Series(dtype=Int64),  # BIGINT

                "SALES_CATEGORY": pd.Series(dtype=object),  # TEXT
                "SALES_VALUE": pd.Series(dtype=float64),  # DECIMAL(22,2) - float64 en uygun seçenektir
                "RETURN_VALUE": pd.Series(dtype=float64),  # DECIMAL(22,2) - float64 en uygun seçenektir
                "AUCTION_NUMBER": pd.Series(dtype=object),  # TEXT

                "TAX_IDENTIFICATION_NUMBER": pd.Series(dtype=Int64),  # BIGINT

            }
        )

        # ============================================================
        # Basic date formats
        # ============================================================

        # Date format patterns organized by year format (4-digit vs 2-digit) and separator type
        date_formats = [
            # 4-digit year formats (YYYY)
            # Hyphen-separated
            "%Y-%m-%d %H%M%S",  # 2024-12-31 235959
            "%Y-%m-%d %H%M",    # 2024-12-31 2359
            "%Y-%m-%d",         # 2024-12-31
            "%d-%m-%Y %H%M%S",  # 31-12-2024 235959
            "%d-%m-%Y %H%M",    # 31-12-2024 2359
            "%d-%m-%Y",         # 31-12-2024
            "%m-%d-%Y %H%M%S",  # 12-31-2024 235959
            "%m-%d-%Y %H%M",    # 12-31-2024 2359
            "%m-%d-%Y",         # 12-31-2024

            # Dot-separated
            "%Y.%m.%d %H%M%S",  # 2024.12.31 235959
            "%Y.%m.%d %H%M",    # 2024.12.31 2359
            "%Y.%m.%d",         # 2024.12.31
            "%d.%m.%Y %H%M%S",  # 31.12.2024 235959
            "%d.%m.%Y %H%M",    # 31.12.2024 2359
            "%d.%m.%Y",         # 31.12.2024
            "%m.%d.%Y %H%M%S",  # 12.31.2024 235959
            "%m.%d.%Y %H%M",    # 12.31.2024 2359
            "%m.%d.%Y",         # 12.31.2024

            # No separator
            "%Y%m%d%H%M%S",     # 20241231235959
            "%Y%m%d%H%M",       # 202412312359
            "%Y%m%d",           # 20241231
            "%d%m%Y%H%M%S",     # 31122024235959
            "%d%m%Y%H%M",       # 311220242359
            "%d%m%Y",           # 31122024
            "%m%d%Y%H%M%S",     # 12312024235959
            "%m%d%Y%H%M",       # 123120242359
            "%m%d%Y",           # 12312024

            # 2-digit year formats (YY)
            # Hyphen-separated
            "%d-%m-%y %H%M%S",  # 31-12-24 235959
            "%d-%m-%y %H%M",    # 31-12-24 2359
            "%d-%m-%y",         # 31-12-24
            "%y-%m-%d %H%M%S",  # 24-12-31 235959
            "%y-%m-%d %H%M",    # 24-12-31 2359
            "%y-%m-%d",         # 24-12-31
            "%m-%d-%y %H%M%S",  # 12-31-24 235959
            "%m-%d-%y %H%M",    # 12-31-24 2359
            "%m-%d-%y",         # 12-31-24

            # Dot-separated
            "%d.%m.%y %H%M%S",  # 31.12.24 235959
            "%d.%m.%y %H%M",    # 31.12.24 2359
            "%d.%m.%y",         # 31.12.24
            "%y.%m.%d %H%M%S",  # 24.12.31 235959
            "%y.%m.%d %H%M",    # 24.12.31 2359
            "%y.%m.%d",         # 24.12.31
            "%m.%d.%y %H%M%S",  # 12.31.24 235959
            "%m.%d.%y %H%M",    # 12.31.24 2359
            "%m.%d.%y",         # 12.31.24

            # No separator
            "%y%m%d%H%M%S",     # 241231235959
            "%y%m%d%H%M",       # 2412312359
            "%y%m%d",           # 241231
            "%d%m%y%H%M%S",     # 311224235959
            "%d%m%y%H%M",       # 3112242359
            "%d%m%y",           # 311224
            "%m%d%y%H%M%S",     # 123124235959
            "%m%d%y%H%M",       # 1231242359
            "%m%d%y",           # 123124
        ]

        # ============================================================
        # Read Excel file from S3 bucket
        # ============================================================

        s3_path = s3_main_path + str(id) + ".xlsx"
        print(f"Debug Reading file from S3 path {s3_path}")


        s3 = s3fs.S3FileSystem()
        with s3.open(s3_path, "rb") as file:    
            excel_data = pd.read_excel(
                file,
                sheet_name="Sheet1",
                dtype=str,
                keep_default_na=False,
                engine="openpyxl",

                header=None,
            )
            print("Debug Excel data read into Pandas DataFrame")


        # ============================================================
        # Convert Pandas DataFrame to Spark DataFrame
        # ============================================================

        veri_Excel_DF_base = sparkSession.createDataFrame(excel_data)
        print("Debug Converted Pandas DataFrame to Spark DataFrame")


        # Optional Convert back to Pandas DataFrame if needed
        veri_Excel_DF = veri_Excel_DF_base.toPandas()
        print("Debug Converted Spark DataFrame to Pandas DataFrame")


        print("File read from S3 bucket and processed successfully")


        # ============================================================
        # Create base1_dataframe1 and base2_dataframe2
        # ============================================================

        # Initialize empty DataFrames
        base1_dataframe1 = pd.DataFrame()
        base2_dataframe2 = pd.DataFrame()

        if structure == TABULAR:
            # ============================================================
            # Remove empty columns
            # ============================================================


            column_headers = veri_Excel_DF.columns.values.tolist()
            print(f"Debug Column headers before removal of empty columns {column_headers}")


            # Loop through columns to remove empty columns
            for column_name in veri_Excel_DF.columns:
                satirDolu = False
                for value in veri_Excel_DF[column_name]:
                    value = str(value).strip()
                    if (
                        value != "" and str(value).lower() != "nan" and str(value).lower() != "nat"
                    ):  # Check if value is not empty, 'nan', or 'nat'
                        satirDolu = True


                # If at least one row is full, add this column to the new DataFrame
                if satirDolu:

                    base1_dataframe1[column_name] = veri_Excel_DF[column_name]

            print("Debug base1_dataframe1 after removing empty columns")
            print(base1_dataframe1)


            # ============================================================
            # Remove empty rows
            # ============================================================

            base2_dataframe2 = pd.DataFrame(columns=base1_dataframe1.columns)
            print("Debug base2_dataframe2 initialized with columns from base1_dataframe1")


            for index, row in base1_dataframe1.iterrows():
                # Combine all values in the row
                concatenated_row = (
                    "".join(row.astype(str)).lower().replace("nan", "").replace("nat", "").replace(" ", "")
                )



                if concatenated_row != "":
                    base2_dataframe2 = base2_dataframe2.append(row)

            print("Debug base2_dataframe2 after removing empty rows")
            print(base2_dataframe2)


        elif structure in ("POSITION", "CUSTOM POSITION", "CUSTOMRAFED"):
            # If structure is POSITION or CUSTOM, just copy the DataFrame
            base2_dataframe2 = veri_Excel_DF.copy()
            print("Debug base2_dataframe2 copied from veri_Excel_DF")



        # ============================================================
        # Finished the first step, base2_dataframe2 was filled
            # ============================================================
        print("Debug Finished processing. base2_dataframe2 has been filled.")


        # ============================================================
        # REMOVE THE LINES CONTAINING SPECIFIC TEXTS LIKE PAGE OF, TOTAL, ETC.
        # ============================================================

        # Define unwanted characters to remove from text
        unwanted_chars = "0123456789 .,=+-()[]"
        translation_table = str.maketrans("", "", unwanted_chars)

        # Define the set of words to check for in rows
        words_to_check = {pageof, page2of2, toplam, total, totalqty}

        # Process each row in base2_dataframe2
        for index, row in base2_dataframe2.iterrows():
            # Concatenate all values in the row into a single string and convert to lowercase
            concatenated_row = "".join(row.astype(str)).lower()


            # Remove 'nan' and 'nat' substrings
            concatenated_row = concatenated_row.replace(nan, ).replace(nat, )

            # Remove unwanted characters
            concatenated_row = concatenated_row.translate(translation_table)

            # Check if the processed row contains any of the unwanted words
            if concatenated_row in words_to_check:
                if structure == TABULAR:
                    # Drop the row if it matches the unwanted criteria
                    base2_dataframe2 = base2_dataframe2.drop(index)

                elif structure in ("POSITION", "CUSTOM POSITION"):
                    # Replace the row with NaN values if it matches the unwanted criteria
                    base2_dataframe2.loc[index] = [np.nan] * len(base2_dataframe2.columns)


        # Reset the index of the DataFrame after modification
        base2_dataframe2.reset_index(drop=True, inplace=True)

        print("Debug Removed unwanted rows and reset index.")
        print("Total, Total, PageOf lines removed or cleared")


        # ============================================================
        # Process Data for KUWAIT Specific Structure
        # ============================================================

        if country == "KUWAIT" and structure == "POSITION":
            # Create a list to hold new rows after processing
            new_rows = []


            for index, row in base2_dataframe2.iterrows():
                # Identify columns that contain '~' to be split
                columns_to_split = [col_base2 for col_base2 in base2_dataframe2.columns if "~" in str(row[col_base2])]


                # If no columns need splitting, append the row as-is
                if not columns_to_split:
                    new_rows.append(row)
                    continue


                # Find the maximum number of splits required for any column
                max_splits = max(len(str(row[col_base2]).split("~")) for col_base2 in columns_to_split)


                list_num_index_column = []

                for i in range(max_splits):
                    new_row = row.copy()
                    for col_base2 in base2_dataframe2.columns:
                        if col_base2 in columns_to_split:
                            split_values = str(row[col_base2]).split("~")
                            if i < len(split_values):
                                new_row[col_base2] = split_values[i]
                            else:

                                new_row[col_base2] = None
                        else:
                            str_value = str(row[col_base2])
                            if str_value.isdigit() and str(col_base2) != 0:

                                exists = any(
                                    item[index] == index and item[col_base2] == col_base2
                                    for item in list_num_index_column
                                )
                                if not exists:
                                    new_row[col_base2] = row[col_base2]
                                    list_num_index_column.append({"index": index, "col_base2": col_base2})
                                else:

                                    new_row[col_base2] = None
                            else:
                                new_row[col_base2] = row[col_base2]
                    new_rows.append(new_row)


            # Update DataFrame with the processed rows
            base2_dataframe2 = pd.DataFrame(new_rows)

            # Optional check for rows still containing '~'
            # contains_tilde = base2_dataframe2.apply(lambda x x.str.contains('~', na=False)).any().any()
            # print(DataFrame'de '~' sembolü içeren satır var mı, contains_tilde)

            print("Converted grouped rows for the ~ symbol for Kuwait into a list")


        # ============================================================
        # Convert Column Headings to Integer and Adjust Index
        # ============================================================

        # Convert column headings to integer
        base2_dataframe2.columns = base2_dataframe2.columns.astype(int)

        # Start column names at 1 instead of 0
        base2_dataframe2.columns = [column + 1 for column in base2_dataframe2.columns]

        # Reset index after modifications
        base2_dataframe2.reset_index(drop=True, inplace=True)

        # Save the DataFrame to CSV
        base2_dataframe2.to_csv(base2_dataframe2.csv, index=False)
        # Optional CSV export with different delimiter and encoding
        # base2_dataframe2.to_csv(base2_dataframe2.csv, sep=';', encoding='utf-8')

        print("base2_dataframe2 column names made Integer")


        # ============================================================
        # FIND MATCHING COLUMNS
        # ============================================================

        print("list_column_match codes started")


        # Initialize the list to store matching columns
        list_column_match = []

        # Initialize counters for mandatory columns
        use_count_second_column_name = 0

        # ============================================================
        # Check if 'second_column_name' is used and count mandatory columns
        # ============================================================

        # Count non-empty second_column_name and etl_column_name
        for row_attribute_detail in result_attribute_detail:
            (
                original_column_name,
                second_column_name,
                etl_column_name,
                column_position,
                starting_row,

                is_mandatory,
            ) = row_attribute_detail
            if second_column_name not in [NA, NULL, None] and etl_column_name not in [NA, NULL, None]:
                use_count_second_column_name += 1


        # Initialize column counters based on context and mandatory status
        column_count_sales_is_mandatory_1 = 0
        column_count_stock_is_mandatory_1 = 0

        # Count mandatory columns based on context and use of second_column_name
        for row_attribute_detail in result_attribute_detail:

            (
                original_column_name,
                second_column_name,
                etl_column_name,
                column_position,
                starting_row,
                is_mandatory,
            ) = row_attribute_detail
            if use_count_second_column_name == 0:
                if original_column_name not in [NA, NULL, None] or etl_column_name not in [NA, NULL, None]:
                    if context == SALES and is_mandatory == 1:
                        column_count_sales_is_mandatory_1 += 1

                    elif context == STOCK and is_mandatory == 1:
                        column_count_stock_is_mandatory_1 += 1

                    elif context == SALESSTOCK and is_mandatory == 1:
                        column_count_sales_is_mandatory_1 += 1
                        column_count_stock_is_mandatory_1 += 1
            else:

                if second_column_name not in [NA, NULL, None] or etl_column_name not in [NA, NULL, None]:
                    if context == SALES and is_mandatory == 1:
                        column_count_sales_is_mandatory_1 += 1

                    elif context == STOCK and is_mandatory == 1:

                        column_count_stock_is_mandatory_1 += 1

                    elif context == SALESSTOCK and is_mandatory == 1:
                        column_count_sales_is_mandatory_1 += 1
                        column_count_stock_is_mandatory_1 += 1


        # ============================================================
        # Populate 'list_column_match' with matching columns
        # ============================================================

        for row_attribute_detail in result_attribute_detail:
            (
                original_column_name,
                second_column_name,
                etl_column_name,
                column_position,
                starting_row,

                is_mandatory,
            ) = row_attribute_detail

            # Determine which column name to use for matching
            searched_excel_column_name = ""

            if use_count_second_column_name == 0:
                if original_column_name not in [NA, NULL, None]:
                    searched_excel_column_name = original_column_name
            else:
                if second_column_name not in [NA, NULL, None]:
                    searched_excel_column_name = second_column_name


            # Clean up the searched column name
            searched_excel_column_name = (
                lambda x: (
                    "NULL" if str(x).strip() in ["", "nan", "NaN", "nat", "NaT", "null", "NULL", None] else x.strip()
                )
                )(searched_excel_column_name)


            if structure == "TABULAR":
                # Search for a match in the DataFrame
                found_match = False
                for indexForRow, row2_base2_dataframe2 in base2_dataframe2.iterrows():


                    for ColumnPositionNo, column_value in row2_base2_dataframe2.items():
                        if str(searched_excel_column_name).strip() == str(column_value).strip():
                            # Check if this combination has been processed before

                            prev_count = len(
                                list(
                                    filter(
                                        lambda x: (indexForRow not in x or x[indexForRow] == indexForRow)
                                        and (
                                            ColumnPositionNo not in x
                                            or x[ColumnPositionNo] == int(ColumnPositionNo)
                                        ),
                                        list_column_match,
                                    )

                                )
                            )
                            if prev_count == 1:
                                print("This row has been processed before")
                            else:

                                # Add match to list
                                list_column_match.append(
                                    {
                                        "structure": structure,
                                        "context": context,
                                        "searched_excel_column_name": str(column_value),
                                        "etl_column_name": etl_column_name,
                                        "indexForRow": int(indexForRow),
                                        "ColumnPositionNo": int(ColumnPositionNo),
                                        "is_mandatory": is_mandatory,

                                    }
                                )
                                found_match = True
                                break
                    if found_match:
                        break
            elif structure in ("POSITION", "CUSTOM POSITION", "CUSTOMRAFED"):
                list_column_match.append(
                    {
                        "structure": structure,
                        "context": context,
                        "searched_excel_column_name": str(searched_excel_column_name),
                        "etl_column_name": etl_column_name,
                        "indexForRow": int(starting_row - 1),
                        "ColumnPositionNo": int(column_position),
                        "is_mandatory": is_mandatory,

                    }
                )

        print("The first phase for list_column_match is finished")


        # ============================================================
        # ADDING THE SALES_QUANTITY COLUMN TO THE LIST for RAFED
        # ============================================================
        if data_owner == "RAFED" and structure == "CUSTOMRAFED" and country == "UAE":
            # Check if 'SALES_QUANTITY' is already in the list_column_match
            print("Checking if 'SALES_QUANTITY' exists in list_column_match.")
            sales_quantity_exists = any(item["etl_column_name"] == "SALES_QUANTITY" for item in list_column_match)


            # If 'SALES_QUANTITY' does not exist, add it to the list
            if not sales_quantity_exists:
                # Find the index for the row where 'etl_column_name' is 'QUANTITY_AVAILABLE'
                print("Finding index for 'QUANTITY_AVAILABLE'.")

                rafed_indexForRow = next(
                    (
                        item[indexForRow]
                        for item in list_column_match
                        if item[etl_column_name] == QUANTITY_AVAILABLE
                    ),
                    0,
                )

                # Determine the column position number
                # Determine the column position number
                print("Determining column position number.")
                rafed_ColumnPositionNo = 0
                if base2_dataframe2 is not None and not base2_dataframe2.empty:
                    rafed_ColumnPositionNo = len(base2_dataframe2.columns) - 10
                    if rafed_ColumnPositionNo < 0:
                        print("Column position number is negative. Exiting.")
                        break

                # Calculate the month and year
                print("Calculating month and year.")
                rafed_year = mail_date.year
                rafed_month = mail_date.month - 1

                if rafed_month == 0:
                    rafed_month = 12
                    rafed_year -= 1

                find_date = rafed_year * 100 + rafed_month

                # Retrieve the specific row from base2_dataframe2
                print(f"Retrieving row {rafed_indexForRow} from base2_dataframe2.")
                row_Rafed = base2_dataframe2.iloc[rafed_indexForRow]

                # Search for the column that matches the calculated date
                print("Searching for matching column based on calculated date.")
                for col_rafed in row_Rafed.index:
                    value_row_rafed = row_Rafed[col_rafed]
                    try:
                        # Attempt to convert value to date
                        date_value = pd.to_datetime(value_row_rafed, errors="raise")
                        # Convert to YYYYMM integer format
                        int_value = date_value.year * 100 + date_value.month

                        # Check if the integer value matches the calculated find_date
                        if int_value == find_date:
                            rafed_ColumnPositionNo = int(col_rafed)
                            print(f"Matching column found: {rafed_ColumnPositionNo}.")
                            break
                    except ValueError:
                        # If conversion fails, continue to the next value
                        continue

                # Print the determined column position number
                print("rafed_ColumnPositionNo: ", rafed_ColumnPositionNo)

                # Update the DataFrame and append the new item to list_column_match
                print("Updating base2_dataframe2 and appending new item to list_column_match.")
                base2_dataframe2[rafed_ColumnPositionNo].iloc[rafed_indexForRow] = "SALES_QUANTITY"

                list_column_match.append(
                    {
                        "structure": structure,
                        "context": context,
                        "searched_excel_column_name": "SALES_QUANTITY",
                        "etl_column_name": "SALES_QUANTITY",
                        "indexForRow": rafed_indexForRow,
                        "ColumnPositionNo": rafed_ColumnPositionNo,
                        "is_mandatory": 0,
                    }
                )
                print("Item 'SALES_QUANTITY' successfully added to list_column_match.")
        # ============================================================
        # STRIP WHITESPACE FROM 'searched_excel_column_name'
        # ============================================================

        list_column_match = [
            {
                "item": item,
                "searched_excel_column_name": item["searched_excel_column_name"].strip(),
            }
            for item in list_column_match

        ]

        # ============================================================
        # RENAMING COLUMN NAMES IN 'list_column_match'
        # ============================================================

        # Change 'QUANTITY_AVAILABLE' to 'AVAILABLE_QUANTITY'
        for item in list_column_match:
            if item["etl_column_name"] and "QUANTITY_AVAILABLE" in item["etl_column_name"]:
                item["etl_column_name"] = "AVAILABLE_QUANTITY"

        # Change 'BATCH_NO' to 'BATCH_NUMBER'
        for item in list_column_match:
            if item["etl_column_name"] and "BATCH_NO" in item["etl_column_name"]:
                item["etl_column_name"] = "BATCH_NUMBER"

        # Change 'REGION' to 'BRANCH_NAME' for specific data owner and structure
        if data_owner == "SURGIPHARM" and structure == "POSITION":
            for item in list_column_match:
                if item["etl_column_name"] and "REGION" in item["etl_column_name"]:
                    item["etl_column_name"] = "BRANCH_NAME"

        # ============================================================
        # APPEND NUMBERS TO DUPLICATE 'searched_excel_column_name' VALUES
        # ============================================================

        # Create a list of 'searched_excel_column_name' values
        searched_names_list_column_match = [item["searched_excel_column_name"] for item in list_column_match]

        # Count occurrences of each name and append a number if duplicates exist
        name_count = {}
        for item in list_column_match:
            name = item["searched_excel_column_name"]
            if searched_names_list_column_match.count(name) > 1:
                if name not in name_count:
                    name_count[name] = 1
                    item["searched_excel_column_name"] = f"{name}1"
                else:
                    name_count[name] += 1
                    item["searched_excel_column_name"] = f"{name}{name_count[name]}"

        # ============================================================
        # COUNT MANDATORY ITEMS
        # ============================================================

        # Count items where 'is_mandatory' is 1
        column_Count_is_mandatory_1 = len(list(filter(lambda x: x["is_mandatory"] == 1, list_column_match)))

        # Count items with 'is_mandatory' 1 based on context
        column_count_matching_sales_is_mandatory_1 = len(
            list(
                filter(
                    lambda x: (x["context"] == "SALES" or x["context"] == "SALESSTOCK") and x["is_mandatory"] == 1,
                    list_column_match
                )
            )
        )

        column_count_matching_stock_is_mandatory_1 = len(
            list(
                filter(
                    lambda x: (x["context"] == "STOCK" or x["context"] == "SALESSTOCK") and x["is_mandatory"] == 1,
                    list_column_match
                )
            )
        )

        # View Column Match List
        print("List column match:", list_column_match)
        print("List column match final phase finished")

        # ============================================================
        # HANDLING HEADER ROW FOR QUIMICA SUIZA
        # ============================================================

        if country == "PERU" and data_owner == "QUIMICA SUIZA" and structure in ("POSITION", "CUSTOM POSITION"):
            print("Adding header row for 'QUIMICA SUIZA' data without header")

            # Count columns in list_column_match and base2_dataframe2
            count_list_column_match = len(list_column_match)
            count_columns_base2_dataframe2 = len(base2_dataframe2.columns)

            if count_list_column_match == count_columns_base2_dataframe2:
                # Create an empty row with the same columns as base2_dataframe2
                empty_row = {}
                for col_base2 in base2_dataframe2.columns:
                    empty_row[col_base2] = None

                # Add the empty row as the first row in the DataFrame
                base2_dataframe2 = pd.concat([pd.DataFrame([empty_row]), base2_dataframe2], ignore_index=True)

                # Fill in the header row with column names from list_column_match
                for item in list_column_match:
                    row_indexMatch = item[indexForRow]
                    ColumnPositionNoMatch = item[ColumnPositionNo]
                    searched_column_nameMatch = item[searched_excel_column_name]

                    base2_dataframe2.at[row_indexMatch, ColumnPositionNoMatch] = searched_column_nameMatch

            print("Header row added successfully")

        # ============================================================
        # CHECKING COLUMN HEADINGS FROM DD_ATTRIBUTE_DETAIL
        # ============================================================

        all_match = True

        if structure in ("POSITION", "CUSTOM POSITION", "CUSTOMRAFED"):
            print("Starting column heading validation")

            for item in list_column_match:
                row_indexMatch = item[indexForRow]
                ColumnPositionNoMatch = item[ColumnPositionNo]
                searched_column_nameMatch = item[searched_excel_column_name]
                is_mandatoryMatch = item[is_mandatory]

                if is_mandatoryMatch == 1:

                    # Check if the specified column and row indices exist in the DataFrame
                    if (ColumnPositionNoMatch not in base2_dataframe2.columns or row_indexMatch not in base2_dataframe2.index):
                        all_match = False
                    else:

                        # If there is a period tag at the beginning of the column, remove the period tag.
                        if (
                            searched_column_nameMatch is not None
                            and searched_column_nameMatch != ""
                            and str(searched_column_nameMatch).lower() not in [nan, nat]
                            and not (isinstance(searched_column_nameMatch, float) and np.isnan(searched_column_nameMatch))
                            and not (isinstance(searched_column_nameMatch, pd.Timestamp) and pd.isna(searched_column_nameMatch))
                        ):
                            # Remove spaces
                            # searched_column_nameMatch = searched_column_nameMatch.replace( , )
                            searched_column_nameMatch = ''.join(searched_column_nameMatch.split())

                            # Remove numeric values
                            searched_column_nameMatch = ''.join(char for char in searched_column_nameMatch if not char.isdigit())

                            # Remove specific characters
                            searched_column_nameMatch = searched_column_nameMatch.replace("_", "").replace("-", "")

                            # Assign NULL if the result is an empty string
                            if searched_column_nameMatch.strip() == "":
                                searched_column_nameMatch = SPECIAL_VALUES['NULL']


                        # Get and clean the column name from base2_dataframe2
                        base2_column_name = str(base2_dataframe2.loc[row_indexMatch, ColumnPositionNoMatch]).strip()

                        if pd.isna(base2_column_name) or base2_column_name.lower() in NULL_VALUES:
                            base2_column_name = SPECIAL_VALUES['NULL']

                        # If there is a period tag at the beginning of the column, remove the period tag.
                        if (
                            base2_column_name is not None
                            and base2_column_name != ""
                            and str(base2_column_name).lower() not in NULL_VALUES
                            and not (isinstance(base2_column_name, float) and np.isnan(base2_column_name))
                            and not (isinstance(base2_column_name, pd.Timestamp) and pd.isna(base2_column_name))
                        ):
                            # Remove spaces
                            base2_column_name = "".join(base2_column_name.split())

                            # Remove numeric values
                            base2_column_name = "".join(char for char in base2_column_name if not char.isdigit())

                            # Remove specific characters
                            base2_column_name = base2_column_name.replace("_", "").replace("-", "")

                            # Assign NULL if the result is an empty string
                            if base2_column_name.strip() == "":
                                base2_column_name = SPECIAL_VALUES['NULL']

                        # Special handling for SURGIPHARM in KENYA context
                        if (data_owner == DATA_OWNERS['SURGIPHARM'] and 
                            country == COUNTRIES['KENYA'] and 
                            context == CONTEXT_TYPES['SALES']):
                            
                            if base2_column_name.startswith("MAINSTORES"):
                                base2_column_name = SPECIAL_VALUES['NULL']
                            if searched_column_nameMatch == "SALESSTOCK":
                                searched_column_nameMatch = SPECIAL_VALUES['NULL']

                        if searched_column_nameMatch != base2_column_name:
                            all_match = False
                            print(f"Mismatch found searched_column_nameMatch {searched_column_nameMatch}")
                            print(f"base2_column_name {base2_column_name}")
                            print(f"id: {id}")
                            break

            # Clear list_column_match if any mismatch is found
            if not all_match:
                list_column_match.clear()
                column_Count_is_mandatory_1 = 0
                column_count_stock_is_mandatory_1 = 0
                column_count_sales_is_mandatory_1 = 0
                column_count_matching_stock_is_mandatory_1 = 0
                column_count_matching_sales_is_mandatory_1 = 0

            print("Column heading validation completed")

        # ============================================================
        # REMOVE HEADER LINES IN ALLIANCE FILE IF THEY REAPPEAR
        # ============================================================

        if (data_owner == DATA_OWNERS['ALLIANCE'] and 
            structure == STRUCTURE_TYPES['POSITION'] and 
            all_match):
            
            print("Processing data for ALLIANCE with POSITION structure.")

            join_base2_first_column_name = ""
            start_index = 0

            # Find headings
            print("Finding headings in list_column_match.")
            for item in list_column_match:
                row_index = item["indexForRow"]
                start_index = row_index
                ColumnPositionNoMatch = item["ColumnPositionNo"]
                join_base2_first_column_name += str(base2_dataframe2.loc[row_index, ColumnPositionNoMatch])

            print(f"Headings concatenated for comparison: {join_base2_first_column_name}")

            join_base2_other_column_name = ""
            start_index += 1

            # Loop through the rows starting from start_index
            print(f"Looping through rows starting from index: {start_index}")
            for row_index in range(start_index, len(base2_dataframe2)):
                if row_index == start_index:
                    join_base2_other_column_name = ""
                    if row_index == 24:
                        # Temporary debug variable (to be removed or used for debugging)
                        asdfasdfa = 1

                    # Concatenate column values to create the comparison string
                    for item in list_column_match:
                        ColumnPositionNoMatch = item["ColumnPositionNo"]
                        join_base2_other_column_name += str(base2_dataframe2.loc[row_index, ColumnPositionNoMatch])

                    # Compare the concatenated values
                    if join_base2_first_column_name == join_base2_other_column_name:
                        print(f"Row {row_index} matches with heading. Removing this row.")
                        # Remove headings
                        base2_dataframe2 = base2_dataframe2.drop(row_index)

            # Reset the index of the DataFrame
            print("Resetting index of base2_dataframe2.")
            base2_dataframe2.reset_index(drop=True, inplace=True)

            print("Processing completed. DataFrame updated.")

        # ============================================================
        # PROCESS COLUMN FILLING FOR SURGIPHARM
        # ============================================================

        if data_owner == SURGIPHARM and structure == POSITION and len(list_column_match) > 0:
            print("Processing data for SURGIPHARM with POSITION structure.")

            # ============================================================
            # FILL 'BRANCH_NAME' COLUMN
            # ============================================================
            print("Filling REGION-BRANCH_NAME Column.")
            ColumnPositionNo_BRANCH_NAME = next(
                (item["ColumnPositionNo"] for item in list_column_match 
                 if item["etl_column_name"] == COLUMN_NAMES['BRANCH_NAME']),
                None
            )
            value_BRANCH_NAME = ""

            if str(ColumnPositionNo_BRANCH_NAME).strip().lower() not in NULL_VALUES:
                for index in range(len(base2_dataframe2) - 1):
                    line_value = str(base2_dataframe2[ColumnPositionNo_BRANCH_NAME].iloc[index])
                    if line_value.startswith("MAIN STORES - "):
                        value_BRANCH_NAME = line_value.replace("MAIN STORES - ", "")
                        print(f"Found branch name {value_BRANCH_NAME}")
                    elif value_BRANCH_NAME:
                        base2_dataframe2[ColumnPositionNo_BRANCH_NAME].iloc[index] = f"SURGIPHARM {value_BRANCH_NAME}"
                        print(f"Updated branch name at row {index}: {base2_dataframe2[ColumnPositionNo_BRANCH_NAME].iloc[index]}")

            # ============================================================
            # FILL PRODUCT_NAME Column
            # ============================================================
            print("Filling PRODUCT_NAME Column.")
            ColumnPositionNo_PRODUCT_NAME = next(
                (item["ColumnPositionNo"] for item in list_column_match 
                 if item["etl_column_name"] == COLUMN_NAMES['PRODUCT_NAME']),
                None
            )
            value_PRODUCT_NAME = ""

            if str(ColumnPositionNo_PRODUCT_NAME).strip().lower() not in NULL_VALUES:
                for index in range(len(base2_dataframe2) - 1):
                    line_value = str(base2_dataframe2[ColumnPositionNo_PRODUCT_NAME].iloc[index])
                    if line_value.lower() not in NULL_VALUES:
                        value_PRODUCT_NAME = line_value
                        print(f"Found product name {value_PRODUCT_NAME}")
                    if value_PRODUCT_NAME:
                        base2_dataframe2[ColumnPositionNo_PRODUCT_NAME].iloc[index] = value_PRODUCT_NAME
                        print(f"Updated product name at row {index}: {base2_dataframe2[ColumnPositionNo_PRODUCT_NAME].iloc[index]}")

            # ============================================================
            # FILL DATA_DATE Column
            # ============================================================
            print("Filling DATA_DATE Column.")
            ColumnPositionNo_DATA_DATE = next(
                (item["ColumnPositionNo"] for item in list_column_match if item["etl_column_name"] == "DATA_DATE"),
                None,
            )
            value_DATA_DATE = ""

            if str(ColumnPositionNo_DATA_DATE).strip().lower() not in ("", "nan", "nat", "none"):
                for index in range(len(base2_dataframe2) - 1):
                    line_value = str(base2_dataframe2[ColumnPositionNo_DATA_DATE].iloc[index])
                    if line_value.lower() not in ("nan", "nat", ""):
                        value_DATA_DATE = line_value
                        print(f"Found data date: {value_DATA_DATE}")
                    if value_DATA_DATE:
                        base2_dataframe2[ColumnPositionNo_DATA_DATE].iloc[index] = value_DATA_DATE
                        print(f"Updated data date at row {index}: {base2_dataframe2[ColumnPositionNo_DATA_DATE].iloc[index]}")

        # ============================================================
        # PROCESS FOR "QUIMICA SUIZA"
        # ============================================================

        if country == "PERU" and data_owner == "QUIMICA SUIZA" and structure in ("POSITION", "CUSTOM POSITION"):
            # Filter rows with specific BRANCH_CODE values
            ColumnPositionNo_BRANCH_CODE = next(
                (item["searched_excel_column_name"] for item in list_column_match if item["etl_column_name"] == "BRANCH_CODE"),
                None
            )

            if ColumnPositionNo_BRANCH_CODE is not None and ColumnPositionNo_BRANCH_CODE in base3_dataframe3.columns:
                filtered_dataframe = base3_dataframe3[
                    base3_dataframe3[ColumnPositionNo_BRANCH_CODE].isin(['3000', '0001'])
                ]
                base3_dataframe3 = filtered_dataframe.copy()

            # Add 'BRANCH_NAME' column if it does not exist and set values based on BRANCH_CODE
            if "BRANCH_NAME" not in base3_dataframe3.columns:
                base3_dataframe3["BRANCH_NAME"] = None

            if ColumnPositionNo_BRANCH_CODE is not None and ColumnPositionNo_BRANCH_CODE in base3_dataframe3.columns:
                base3_dataframe3["BRANCH_NAME"] = base3_dataframe3[ColumnPositionNo_BRANCH_CODE].apply(
                    lambda x: "QUIMICA SUIZA PUBLIC" if x == "3000" else ("QUIMICA SUIZA PRIVATE" if x == "0001" else x)
                )

                # Add BRANCH_NAME to list_column_match if not already present
                branch_name_exists = any(item["etl_column_name"] == "BRANCH_NAME" for item in list_column_match)

                if not branch_name_exists:
                    list_column_match.append(
                        {
                            "structure": structure,
                            "context": context,
                            "searched_excel_column_name": "BRANCH_NAME",
                            "etl_column_name": "BRANCH_NAME",
                            "indexForRow": next(
                                (item["indexForRow"] for item in list_column_match if item["etl_column_name"] == "BRANCH_CODE"),
                                0
                            ),
                            "ColumnPositionNo": "BRANCH_NAME",
                            "is_mandatory": 0
                        }
                    )

            base3_dataframe3.reset_index(drop=True, inplace=True)

        # ================================
        # SECTION: QUIMICA SUIZA STOCK CONTEXT
        # ================================

        if (
            country == "PERU"
            and data_owner == "QUIMICA SUIZA"
            and structure in ("POSITION", "CUSTOM POSITION")
            and context == "STOCK"
        ):
            print("Processing QUIMICA SUIZA STOCK context...")

            # Fill NULL values in 'BATCH_NUMBER' column with 'N/A'
            ColumnPositionNo_BATCH_NUMBER = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "BATCH_NUMBER"
                ),
                None,
            )

            if ColumnPositionNo_BATCH_NUMBER is not None and ColumnPositionNo_BATCH_NUMBER in base3_dataframe3.columns:
                print(f"Filling NULL values in '{ColumnPositionNo_BATCH_NUMBER}' with 'N/A'.")
                base3_dataframe3[ColumnPositionNo_BATCH_NUMBER] = base3_dataframe3[
                    ColumnPositionNo_BATCH_NUMBER
                ].str.strip()
                base3_dataframe3[ColumnPositionNo_BATCH_NUMBER].replace(["", None, np.nan, "null"], "N/A", inplace=True)
                print("NULL values filled successfully.")

            # Check if 'INVENTORY_CATEGORY' column exists; if not, add it
            if "INVENTORY_CATEGORY" not in base3_dataframe3.columns:
                print("Adding 'INVENTORY_CATEGORY' column with default value 'GN'.")
                base3_dataframe3["INVENTORY_CATEGORY"] = "GN"

            if "INVENTORY_CATEGORY" in base3_dataframe3.columns and "BRANCH_NAME" in base3_dataframe3.columns:
                print("Updating 'INVENTORY_CATEGORY' based on 'BRANCH_NAME'.")
                base3_dataframe3["INVENTORY_CATEGORY"] = base3_dataframe3["BRANCH_NAME"].apply(
                    lambda x: (
                        "PR" if x == "QUIMICA SUIZA PRIVATE" else ("PU" if x == "QUIMICA SUIZA PUBLIC" else "GN")
                    )
                )
                print("'INVENTORY_CATEGORY' updated successfully.")

                # Check and append 'INVENTORY_CATEGORY' to the list if not present
                inventory_category_exists = any(
                    item["etl_column_name"] == "INVENTORY_CATEGORY" for item in list_column_match
                )
                if not inventory_category_exists:
                    print("Appending 'INVENTORY_CATEGORY' to list_column_match.")
                    list_column_match.append(
                        {
                            "structure": structure,
                            "context": context,
                            "searched_excel_column_name": "INVENTORY_CATEGORY",
                            "etl_column_name": "INVENTORY_CATEGORY",
                            "indexForRow": next(
                                (
                                    item["indexForRow"]
                                    for item in list_column_match
                                    if item["etl_column_name"] == "BRANCH_NAME"
                                ),
                                0,
                            ),
                            "ColumnPositionNo": "INVENTORY_CATEGORY",
                            "is_mandatory": 0,
                        }
                    )
                print("'INVENTORY_CATEGORY' successfully appended to list_column_match.")

            # Process and calculate 'BLOCKED_QUANTITY'
            ColumnPositionNo_QUANTITY_IN_TRANSIT1 = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "QUANTITY_IN_TRANSIT1"
                ),
                None,
            )

            ColumnPositionNo_QUANTITY_IN_TRANSIT2 = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "QUANTITY_IN_TRANSIT2"
                ),
                None,
            )

            if "BLOCKED_QUANTITY" not in base3_dataframe3.columns:
                print("Adding 'BLOCKED_QUANTITY' column with default value 0.")
                base3_dataframe3["BLOCKED_QUANTITY"] = 0

            # Ensure 'QUANTITY_IN_TRANSIT1' and 'QUANTITY_IN_TRANSIT2' are numeric
            if (
                ColumnPositionNo_QUANTITY_IN_TRANSIT1
                and ColumnPositionNo_QUANTITY_IN_TRANSIT1 in base3_dataframe3.columns
            ):
                print("Converting 'QUANTITY_IN_TRANSIT1' to numeric.")
                base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT1] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT1],
                    errors="coerce",
                )
            else:
                base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT1] = 0

            if (
                ColumnPositionNo_QUANTITY_IN_TRANSIT2
                and ColumnPositionNo_QUANTITY_IN_TRANSIT2 in base3_dataframe3.columns
            ):
                print("Converting 'QUANTITY_IN_TRANSIT2' to numeric.")
                base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT2] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT2],
                    errors="coerce",
                )
            else:
                base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT2] = 0

            print("Calculating 'BLOCKED_QUANTITY'.")
            base3_dataframe3["BLOCKED_QUANTITY"] = base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT1].fillna(
                0
            ) + base3_dataframe3[ColumnPositionNo_QUANTITY_IN_TRANSIT2].fillna(0)
            base3_dataframe3["BLOCKED_QUANTITY"] = base3_dataframe3["BLOCKED_QUANTITY"] / 1000
            print("'BLOCKED_QUANTITY' calculation complete.")

            # Check and append 'BLOCKED_QUANTITY' to the list if not present
            blocked_quantity_exists = any(item["etl_column_name"] == "BLOCKED_QUANTITY" for item in list_column_match)
            if not blocked_quantity_exists:
                print("Appending 'BLOCKED_QUANTITY' to list_column_match.")
                list_column_match.append(
                    {
                        "structure": structure,
                        "context": context,
                        "searched_excel_column_name": "BLOCKED_QUANTITY",
                        "etl_column_name": "BLOCKED_QUANTITY",
                        "indexForRow": 0,
                        "ColumnPositionNo": "BLOCKED_QUANTITY",
                        "is_mandatory": 0,
                    }
                )
            print("'BLOCKED_QUANTITY' successfully appended to list_column_match.")

            # Validate 'AVAILABLE_QUANTITY' column and process data
            ColumnPositionNo_AVAILABLE_QUANTITY = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "AVAILABLE_QUANTITY"
                ),
                None,
            )

            if (
                ColumnPositionNo_AVAILABLE_QUANTITY is not None
                and ColumnPositionNo_AVAILABLE_QUANTITY in base3_dataframe3.columns
            ):
                print("Converting 'AVAILABLE_QUANTITY' to numeric and scaling.")
                base3_dataframe3[ColumnPositionNo_AVAILABLE_QUANTITY] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_AVAILABLE_QUANTITY],
                    errors="coerce",
                )
                base3_dataframe3 = base3_dataframe3.dropna(subset=[ColumnPositionNo_AVAILABLE_QUANTITY])
                base3_dataframe3 = base3_dataframe3[base3_dataframe3[ColumnPositionNo_AVAILABLE_QUANTITY] >= 0]
                print("'AVAILABLE_QUANTITY' processed and cleaned.")

        # ================================
        # SECTION: QUIMICA SUIZA SALES CONTEXT
        # ================================

        if (
            country == "PERU"
            and data_owner == "QUIMICA SUIZA"
            and structure in ("POSITION", "CUSTOM POSITION")
            and context == "SALES"
        ):
            print("Processing QUIMICA SUIZA SALES context...")

            # Check if 'SALES_CATEGORY' column exists; if not, add it
            if "SALES_CATEGORY" not in base3_dataframe3.columns:
                print("Adding 'SALES_CATEGORY' column with default value 'GN'.")
                base3_dataframe3["SALES_CATEGORY"] = "GN"

            if "SALES_CATEGORY" in base3_dataframe3.columns and "BRANCH_NAME" in base3_dataframe3.columns:
                print("Updating 'SALES_CATEGORY' based on 'BRANCH_NAME'.")
                base3_dataframe3["SALES_CATEGORY"] = base3_dataframe3["BRANCH_NAME"].apply(
                    lambda x: (
                        "PR" if x == "QUIMICA SUIZA PRIVATE" else ("PU" if x == "QUIMICA SUIZA PUBLIC" else "GN")
                    )
                )
                print("'SALES_CATEGORY' updated successfully.")

                # Check and append 'SALES_CATEGORY' to the list if not present
                inventory_category_exists = any(
                    item["etl_column_name"] == "SALES_CATEGORY" for item in list_column_match
                )
                if not inventory_category_exists:
                    print("Appending 'SALES_CATEGORY' to list_column_match.")
                    list_column_match.append(
                        {
                            "structure": structure,
                            "context": context,
                            "searched_excel_column_name": "SALES_CATEGORY",
                            "etl_column_name": "SALES_CATEGORY",
                            "indexForRow": next(
                                (
                                    item["indexForRow"]
                                    for item in list_column_match
                                    if item["etl_column_name"] == "BRANCH_NAME"
                                ),
                                0,
                            ),
                            "ColumnPositionNo": "SALES_CATEGORY",
                            "is_mandatory": 0,
                        }
                    )
                print("'SALES_CATEGORY' successfully appended to list_column_match.")

            # Process and scale 'SALES_QUANTITY'
            ColumnPositionNo_SALES_QUANTITY = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "SALES_QUANTITY"
                ),
                None,
            )

            if (
                ColumnPositionNo_SALES_QUANTITY is not None
                and ColumnPositionNo_SALES_QUANTITY in base3_dataframe3.columns
            ):
                print("Converting 'SALES_QUANTITY' to numeric and scaling.")
                base3_dataframe3[ColumnPositionNo_SALES_QUANTITY] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_SALES_QUANTITY], errors="coerce"
                )
                print("SALES_QUANTITY after conversion and scaling:")
                print(base3_dataframe3)

                base3_dataframe3.loc[
                    base3_dataframe3[ColumnPositionNo_SALES_QUANTITY].notna(),
                    ColumnPositionNo_SALES_QUANTITY,
                ] = (
                    base3_dataframe3[ColumnPositionNo_SALES_QUANTITY] / 1000
                )
                print("SALES_QUANTITY after scaling:")
                print(base3_dataframe3)

            # Process and scale 'RETURN_QUANTITY'
            ColumnPositionNo_RETURN_QUANTITY = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "RETURN_QUANTITY"
                ),
                None,
            )

            if (
                ColumnPositionNo_RETURN_QUANTITY is not None
                and ColumnPositionNo_RETURN_QUANTITY in base3_dataframe3.columns
            ):
                print("Converting 'RETURN_QUANTITY' to numeric and scaling.")
                base3_dataframe3[ColumnPositionNo_RETURN_QUANTITY] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_RETURN_QUANTITY], errors="coerce"
                )
                print("RETURN_QUANTITY after conversion and scaling:")
                print(base3_dataframe3)

                base3_dataframe3.loc[
                    base3_dataframe3[ColumnPositionNo_RETURN_QUANTITY].notna(),
                    ColumnPositionNo_RETURN_QUANTITY,
                ] = (
                    base3_dataframe3[ColumnPositionNo_RETURN_QUANTITY] / 1000
                )
                print("RETURN_QUANTITY after scaling:")
                print(base3_dataframe3)

            # Process and scale 'SALES_VALUE'
            ColumnPositionNo_SALES_VALUE = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "SALES_VALUE"
                ),
                None,
            )

            if ColumnPositionNo_SALES_VALUE is not None and ColumnPositionNo_SALES_VALUE in base3_dataframe3.columns:
                print("Converting 'SALES_VALUE' to numeric and scaling.")
                base3_dataframe3[ColumnPositionNo_SALES_VALUE] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_SALES_VALUE], errors="coerce"
                )
                print("SALES_VALUE after conversion and scaling:")
                print(base3_dataframe3)

                base3_dataframe3.loc[
                    base3_dataframe3[ColumnPositionNo_SALES_VALUE].notna(),
                    ColumnPositionNo_SALES_VALUE,
                ] = (
                    base3_dataframe3[ColumnPositionNo_SALES_VALUE] / 1000
                )
                print("SALES_VALUE after scaling:")
                print(base3_dataframe3)

            # Process and scale 'RETURN_VALUE'
            ColumnPositionNo_RETURN_VALUE = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "RETURN_VALUE"
                ),
                None,
            )

            if ColumnPositionNo_RETURN_VALUE is not None and ColumnPositionNo_RETURN_VALUE in base3_dataframe3.columns:
                print("Converting 'RETURN_VALUE' to numeric and scaling.")
                base3_dataframe3[ColumnPositionNo_RETURN_VALUE] = pd.to_numeric(
                    base3_dataframe3[ColumnPositionNo_RETURN_VALUE], errors="coerce"
                )
                print("RETURN_VALUE after conversion and scaling:")
                print(base3_dataframe3)

                base3_dataframe3.loc[
                    base3_dataframe3[ColumnPositionNo_RETURN_VALUE].notna(),
                    ColumnPositionNo_RETURN_VALUE,
                ] = (
                    base3_dataframe3[ColumnPositionNo_RETURN_VALUE] / 1000
                )
                print("RETURN_VALUE after scaling:")
                print(base3_dataframe3)

        base3_dataframe3.reset_index(drop=True, inplace=True)

        if (
            country == "PERU"
            and data_owner == "QUIMICA SUIZA"
            and structure in ("POSITION", "CUSTOM POSITION")
            and context == "SALES"
            and len(base3_dataframe3) > 0
        ):
            print("Starting product join process for 'QUIMICA SUIZA' sales...")

            # Define SQL query for product lookup
            query_LKP_PERU_PRODUCT = (
                "SELECT CAST(CAST(product_id AS INTEGER) AS TEXT) AS lkp_product_id, "
                "product_name AS lkp_product_name FROM " + schemaname + ".lkp_peru_product"
            )
            print("SQL Query for product lookup:", query_LKP_PERU_PRODUCT)

            # Read data from JDBC source
            source_df = (
                sparkSession.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", query_LKP_PERU_PRODUCT)
                .option("driver", "org.postgresql.Driver")
                .option("user", username)
                .option("password", password)
                .load()
            )

            # Convert Spark DataFrame to DynamicFrame
            dynamic_frame_lkp_product = DynamicFrame.fromDF(source_df, glueContext, "dynamic_df")
            print("DynamicFrame created for product lookup.")

            # Select required columns
            dynamic_frame_lkp_product = dynamic_frame_lkp_product.select_fields(["lkp_product_id", "lkp_product_name"])

            # Convert DynamicFrame to Pandas DataFrame
            pandas_df_lkp_product = dynamic_frame_lkp_product.toDF().toPandas()
            print("Converted DynamicFrame to Pandas DataFrame for product lookup.")

            # Strip leading zeros from PRODUCT_ID in base3_dataframe3
            searched_excel_column_name_PRODUCT_ID = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "PRODUCT_ID"
                ),
                None,
            )

            base3_dataframe3[searched_excel_column_name_PRODUCT_ID] = base3_dataframe3[
                searched_excel_column_name_PRODUCT_ID
            ].str.lstrip("0")
            print("Stripped leading zeros from 'PRODUCT_ID' in base3_dataframe3.")

            # Merge DataFrames
            merged_dataframe3 = pd.merge(
                base3_dataframe3,
                pandas_df_lkp_product,
                left_on=searched_excel_column_name_PRODUCT_ID,
                right_on="lkp_product_id",
                how="inner",
            )
            print("DataFrames merged on 'PRODUCT_ID'.")

            # Clean up merged DataFrame
            if "lkp_product_id" in merged_dataframe3.columns:
                merged_dataframe3 = merged_dataframe3.drop(columns=["lkp_product_id"])
                print("Dropped 'lkp_product_id' column from merged DataFrame.")

            if "PRODUCT_NAME" in merged_dataframe3.columns:
                merged_dataframe3 = merged_dataframe3.drop(columns=["PRODUCT_NAME"])
                print("Dropped 'PRODUCT_NAME' column from merged DataFrame.")

            if "lkp_product_name" in merged_dataframe3.columns:
                merged_dataframe3.rename(columns={"lkp_product_name": "PRODUCT_NAME"}, inplace=True)
                print("Renamed 'lkp_product_name' to 'PRODUCT_NAME' in merged DataFrame.")

            # Update base3_dataframe3
            base3_dataframe3 = merged_dataframe3

            # Check if 'PRODUCT_NAME' column exists in list_column_match
            product_name_exists = any(item["etl_column_name"] == "PRODUCT_NAME" for item in list_column_match)
            if not product_name_exists:
                print("Appending 'PRODUCT_NAME' to list_column_match.")
                list_column_match.append(
                    {
                        "structure": structure,
                        "context": context,
                        "searched_excel_column_name": "PRODUCT_NAME",
                        "etl_column_name": "PRODUCT_NAME",
                        "indexForRow": next(
                            (
                                item["indexForRow"]
                                for item in list_column_match
                                if item["etl_column_name"] == "PRODUCT_ID"
                            ),
                            0,
                        ),
                        "ColumnPositionNo": "PRODUCT_NAME",
                        "is_mandatory": 0,
                    }
                )
            print("'PRODUCT_NAME' successfully appended to list_column_match.")

        base3_dataframe3.reset_index(drop=True, inplace=True)
        print("Reset index for base3_dataframe3 after product join.")

        # ================================
        # SECTION: CUSTOMER JOIN PROCESS OF SALES OF "QUIMICA SUIZA"
        # ================================

        if (
            country == "PERU"
            and data_owner == "QUIMICA SUIZA"
            and structure in ("POSITION", "CUSTOM POSITION")
            and context == "SALES"
            and len(base3_dataframe3) > 0
        ):
            print("Starting customer join process for 'QUIMICA SUIZA' sales...")

            # Define SQL query for maximum client ID
            query_max_id_for_client = (
                "SELECT MAX(id) AS max_id FROM " + schemaname + ".daq_log_info WHERE LOWER(file) LIKE '%client%'"
            )
            print("SQL Query for maximum client ID:", query_max_id_for_client)

            # Read data from JDBC source
            source_df = (
                sparkSession.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", query_max_id_for_client)
                .option("driver", "org.postgresql.Driver")
                .option("user", username)
                .option("password", password)
                .load()
            )

            # Convert Spark DataFrame to DynamicFrame
            dynamic_dframe = DynamicFrame.fromDF(source_df, glueContext, "dynamic_df")
            print("DynamicFrame created for client ID query.")

            # Extract max ID from DynamicFrame
            result_daq_log_info = dynamic_dframe.select_fields(["max_id"]).toDF().collect()
            max_id = result_daq_log_info[0]["max_id"] if result_daq_log_info else None
            print("Max client ID obtained:", max_id)

            # s3_path_max = "s3://roche-ereteam/IMIP_Excel_file_to_DB/" + str(max_id) + ".xlsx"
            s3_path_max = s3_main_path + str(max_id) + ".xlsx"
            print(f"Debug: Reading file from S3 path max: {s3_path_max}")

            # Read Excel data from S3
            s3 = s3fs.S3FileSystem()
            with s3.open(
                s3_path_max,
                "rb",
            ) as file:
                excel_data_customer = pd.read_excel(
                    file,
                    sheet_name="Sheet1",
                    dtype=str,
                    keep_default_na=False,
                    engine="openpyxl",
                    header=None,
                )
            print("Excel data for customer read from S3.")

            # Convert Pandas DataFrame to Spark DataFrame
            veri_Excel_Customer_base = sparkSession.createDataFrame(excel_data_customer)
            veri_Excel_Customer_DF = veri_Excel_Customer_base.toPandas()
            print("Converted Excel data to Pandas DataFrame for customer.")

            # Define column specifications for parsing
            colspecs = [
                (0, 19),
                (19, 27),
                (27, 33),
                (33, 73),
                (73, 93),
                (93, 105),
                (105, 165),
                (165, 175),
                (175, 185),
                (185, 225),
                (225, 265),
                (265, 305),
                (305, 325),
                (325, 333),
                (333, 341),
                (341, 345),
                (345, 347),
                (347, 349),
                (349, 357),
                (357, 359),
                (359, 363),
            ]

            # Create regex pattern for column splitting
            regex_pattern = "".join([f"(.{{{end - start}}})" for start, end in colspecs])
            print("Regex pattern for column splitting:", regex_pattern)

            # Define function to split columns using regex
            def split_columns_using_regex(row):
                match = re.match(regex_pattern, row[0])
                if match:
                    return match.groups()
                return [None] * len(colspecs)

            # Apply regex function to split columns
            split_data = veri_Excel_Customer_DF.apply(split_columns_using_regex, axis=1)
            veri_Excel_Customer_DF = pd.DataFrame(split_data.tolist())
            print("Split columns in customer DataFrame.")

            # Rename columns
            veri_Excel_Customer_DF.columns = veri_Excel_Customer_DF.columns.astype(str)

            if "1" in veri_Excel_Customer_DF.columns:
                veri_Excel_Customer_DF.rename(columns={"1": "lkp_customer_id"}, inplace=True)
            if "3" in veri_Excel_Customer_DF.columns:
                veri_Excel_Customer_DF.rename(columns={"3": "lkp_customer_name"}, inplace=True)

            # Get searched column name for CUSTOMER_ID
            searched_excel_column_name_CUSTOMER_ID = next(
                (
                    item["searched_excel_column_name"]
                    for item in list_column_match
                    if item["etl_column_name"] == "CUSTOMER_ID"
                ),
                None,
            )

            print("Searched column name for CUSTOMER_ID:", searched_excel_column_name_CUSTOMER_ID)

            # Strip leading zeros from CUSTOMER_ID in customer DataFrame
            veri_Excel_Customer_DF["lkp_customer_id"] = veri_Excel_Customer_DF["lkp_customer_id"].str.lstrip("0")
            print("Stripped leading zeros from 'lkp_customer_id' in customer DataFrame.")

            # Update CUSTOMER_ID in base3_dataframe3
            base3_dataframe3[searched_excel_column_name_CUSTOMER_ID] = base3_dataframe3[
                searched_excel_column_name_CUSTOMER_ID
            ].str.lstrip("0")

            # Merge DataFrames
            merged_dataframe3 = pd.merge(
                base3_dataframe3,
                veri_Excel_Customer_DF,
                left_on=searched_excel_column_name_CUSTOMER_ID,
                right_on="lkp_customer_id",
                how="inner",
            )
            print("DataFrames merged on 'CUSTOMER_ID'.")

            # Clean up merged DataFrame
            if "lkp_customer_id" in merged_dataframe3.columns:
                merged_dataframe3 = merged_dataframe3.drop(columns=["lkp_customer_id"])
                print("Dropped 'lkp_customer_id' column from merged DataFrame.")

            if "CUSTOMER_NAME" in merged_dataframe3.columns:
                merged_dataframe3 = merged_dataframe3.drop(columns=["CUSTOMER_NAME"])
                print("Dropped 'CUSTOMER_NAME' column from merged DataFrame.")

            if "lkp_customer_name" in merged_dataframe3.columns:
                merged_dataframe3.rename(columns={"lkp_customer_name": "CUSTOMER_NAME"}, inplace=True)
                print("Renamed 'lkp_customer_name' to 'CUSTOMER_NAME' in merged DataFrame.")

            # Update base3_dataframe3
            base3_dataframe3 = merged_dataframe3

            # Check if 'CUSTOMER_NAME' column exists in list_column_match
            customer_name_exists = any(item["etl_column_name"] == "CUSTOMER_NAME" for item in list_column_match)
            if not customer_name_exists:
                print("Appending 'CUSTOMER_NAME' to list_column_match.")
                list_column_match.append(
                    {
                        "structure": structure,
                        "context": context,
                        "searched_excel_column_name": "CUSTOMER_NAME",
                        "etl_column_name": "CUSTOMER_NAME",
                        "indexForRow": next(
                            (
                                item["indexForRow"]
                                for item in list_column_match
                                if item["etl_column_name"] == "CUSTOMER_ID"
                            ),
                            0,
                        ),
                        "ColumnPositionNo": "CUSTOMER_NAME",
                        "is_mandatory": 0,
                    }
                )
            print("'CUSTOMER_NAME' successfully appended to list_column_match.")

            base3_dataframe3.reset_index(drop=True, inplace=True)
            print("Reset index for base3_dataframe3 after customer join.")
            print("Final DataFrame after customer join:")
            print(base3_dataframe3)


        # ================================
        # SECTION STOCK AND SALESSTOCK CONTEXT PROCESSING
        # ================================

        if (
            context in [STOCK, SALESSTOCK]
            and len(base3_dataframe3)  0
            and (column_count_stock_is_mandatory_1 == column_count_matching_stock_is_mandatory_1)
            and (column_count_matching_stock_is_mandatory_1 != 0)
        )
            stock_list_column_match = list_column_match
            print(Starting stock processing...)

            # ================================
            # INTERMEDIATE SECTION CITYPHARMACY STOCK CONTEXT
            # ================================

            if data_owner == CITYPHARMACY and context == STOCK
                print(Processing CITYPHARMACY stock data...)

                # Find 'searched_excel_column_name' for 'STOCK_CATEGORY'
                searched_excel_column_name_STOCK_CATEGORY = next(
                    (
                        item[searched_excel_column_name]
                        for item in stock_list_column_match
                        if item[etl_column_name] == STOCK_CATEGORY
                    ),
                    None,
                )
                print(Searched column name for STOCK_CATEGORY, searched_excel_column_name_STOCK_CATEGORY)

                # Filter out rows where STOCK_CATEGORY is 'I'
                base3_dataframe3 = base3_dataframe3[base3_dataframe3[searched_excel_column_name_STOCK_CATEGORY] != I]
                print(Filtered out rows where STOCK_CATEGORY is 'I'.)

                # Reset index to start from zero
                base3_dataframe3 = base3_dataframe3.reset_index(drop=True)
                print(Index reset for base3_dataframe3.)

            # ================================
            # INTERMEDIATE SECTION REMOVE EMPTY ROWS IN AVAILABLE_QUANTITY
            # ================================

            # Find 'searched_excel_column_name' for 'AVAILABLE_QUANTITY'
            searched_excel_column_name_AVAILABLE_QUANTITY = next(
                (
                    item[searched_excel_column_name]
                    for item in stock_list_column_match
                    if item[etl_column_name] == AVAILABLE_QUANTITY
                ),
                None,
            )
            print(Searched column name for AVAILABLE_QUANTITY, searched_excel_column_name_AVAILABLE_QUANTITY)

            if not searched_excel_column_name_AVAILABLE_QUANTITY
                raise ValueError(Column 'AVAILABLE_QUANTITY' not found.)

            # Remove rows with NaN, NaT, None, null, or empty string values
            valid_entries = base3_dataframe3[
                base3_dataframe3[searched_excel_column_name_AVAILABLE_QUANTITY].apply(
                    lambda x not (pd.isna(x) or str(x).strip().lower() in [nan, nat, none, null, ])
                )
            ]
            print(Removed empty rows in AVAILABLE_QUANTITY column.)

            # Reset index
            valid_entries.reset_index(drop=True, inplace=True)
            stock_base3_dataframe3 = valid_entries.copy()
            print(Index reset and data copied to stock_base3_dataframe3.)

            # ================================
            # INTERMEDIATE SECTION TRANSFER ETL COLUMN NAMES TO 'matched_columns'
            # ================================

            matched_columns = [
                item[etl_column_name] for item in stock_list_column_match if item[etl_column_name] is not None
            ]
            print(Matched columns, matched_columns)

            # ================================
            # INTERMEDIATE SECTION CHECK AND ASSIGN VALUES FOR RESULT DATAFRAME
            # ================================

            for column_name in df_Result_Stock.columns
                if column_name in matched_columns
                    find_searched_excel_column_name = None
                    find_etl_column_name = None
                    for item in stock_list_column_match
                        if item[etl_column_name] == column_name
                            find_searched_excel_column_name = item[searched_excel_column_name]
                            if column_name == AVAILABLE_QUANTITY or column_name == BLOCKED_QUANTITY
                                stock_base3_dataframe3[find_searched_excel_column_name].fillna(
                                    0, inplace=True
                                )
                                numeric_values = pd.to_numeric(
                                    stock_base3_dataframe3[find_searched_excel_column_name],
                                    errors=coerce,
                                )
                                df_Result_Stock[column_name] = numeric_values.fillna(0).astype(int)
                            elif column_name == DATA_DATE or column_name == EXPIRY_DATE
                                df_Result_Stock[column_name] = pd.to_datetime(
                                    stock_base3_dataframe3[find_searched_excel_column_name]
                                )
                            else
                                df_Result_Stock[column_name] = stock_base3_dataframe3[find_searched_excel_column_name]
                            print(fProcessed column {column_name})
                            break

            # ================================
            # INTERMEDIATE SECTION HANDLE UNMATCHED COLUMNS
            # ================================

            for column_name in df_Result_Stock.columns
                if column_name not in matched_columns
                    if column_name == DATA_DATE
                        df_Result_Stock[column_name] = data_date
                    elif column_name == COUNTRY_NAME
                        df_Result_Stock[column_name] = country
                    elif column_name == ORGANIZATION_NAME
                        df_Result_Stock[column_name] = organization_name
                    elif column_name == BRANCH_NAME
                        if data_owner in [BEK, ISKOOP, SELÇUK, ALLIANCE]
                            df_Result_Stock[column_name] = stock_base3_dataframe3[apparo_branch_name]
                        else
                            df_Result_Stock[column_name] = branch_name
                    elif column_name == PRODUCT_ID
                        df_Result_Stock[column_name] = NULL
                    elif column_name == PRODUCT_NAME
                        df_Result_Stock[column_name] = NULL
                    elif column_name == AVAILABLE_QUANTITY
                        df_Result_Stock[column_name] = 0
                    elif column_name == BLOCKED_QUANTITY
                        df_Result_Stock[column_name] = 0
                    elif column_name == INVENTORY_CATEGORY
                        df_Result_Stock[column_name] = GN
                    elif column_name == BATCH_NUMBER
                        df_Result_Stock[column_name] = NA
                    elif column_name == EXPIRY_DATE
                        df_Result_Stock[column_name] = pd.to_datetime(2199-12-31 000000, format=%Y-%m-%d %H%M%S)
                    print(fHandled unmatched column {column_name})

            # ================================
            # INTERMEDIATE SECTION SPECIAL CASE FOR RAFED
            # ================================

            if data_owner == RAFED and structure == CUSTOMRAFED and country == UAE
                if daq_sheet_name == Tawam Data
                    df_Result_Stock[ORGANIZATION_NAME] = TAWAM
                elif daq_sheet_name == Mafraq-SSMC Data
                    df_Result_Stock[ORGANIZATION_NAME] = SSMC
                print(Updated ORGANIZATION_NAME for RAFED special case.)

            # ================================
            # INTERMEDIATE SECTION CITY PHARMACY SPECIAL CASE
            # ================================

            if data_owner == CITYPHARMACY and context == STOCK
                print(Processing CITYPHARMACY special case...)

                # Update 'REGION_NAME' column
                searched_excel_column_name_REGION = next(
                    (
                        item[searched_excel_column_name]
                        for item in stock_list_column_match
                        if item[etl_column_name] == REGION
                    ),
                    None,
                )
                df_Result_Stock[REGION_NAME] = stock_base3_dataframe3[searched_excel_column_name_REGION]
                df_Result_Stock[REGION_NAME] = df_Result_Stock[REGION_NAME].str.upper().str.replace(İ, I).str.split( ).str[0]

                # Update 'BRANCH_NAME' column
                df_Result_Stock[BRANCH_NAME] = df_Result_Stock.apply(
                    lambda row (
                        row[ORGANIZATION_NAME] +  SHARJAH
                        if pd.notna(row[REGION_NAME]) and row[COUNTRY_NAME] == UAE
                        and row[ORGANIZATION_NAME] == CITYPHARMACY
                        and row[REGION_NAME].upper() == SHARJAH
                        else (
                            row[ORGANIZATION_NAME] +  KIZAD
                            if pd.notna(row[REGION_NAME]) and row[COUNTRY_NAME] == UAE
                            and row[ORGANIZATION_NAME] == CITYPHARMACY
                            and row[REGION_NAME].upper() != SHARJAH
                            else (
                                row[ORGANIZATION_NAME] +  KIZAD
                                if pd.isna(row[REGION_NAME]) and row[COUNTRY_NAME] == UAE
                                and row[ORGANIZATION_NAME] == CITYPHARMACY
                                else row[BRANCH_NAME]
                            )
                        )
                    ),
                    axis=1,
                )
                df_Result_Stock = df_Result_Stock.drop(columns=[REGION_NAME])

                # Update 'INVENTORY_CATEGORY' based on 'STOCK_CATEGORY'
                searched_excel_column_name_STOCK_CATEGORY = next(
                    (
                        item[searched_excel_column_name]
                        for item in stock_list_column_match
                        if item[etl_column_name] == STOCK_CATEGORY
                    ),
                    None,
                )
                df_Result_Stock[STOCK_CATEGORY] = stock_base3_dataframe3[searched_excel_column_name_STOCK_CATEGORY]
                df_Result_Stock[INVENTORY_CATEGORY] = df_Result_Stock.apply(
                    lambda row (
                        PU
                        if row[ORGANIZATION_NAME] == CITYPHARMACY and row[STOCK_CATEGORY] == I
                        else (
                            PR
                            if row[ORGANIZATION_NAME] == CITYPHARMACY and row[STOCK_CATEGORY] == N
                            else row[INVENTORY_CATEGORY]
                        )
                    ),
                    axis=1,
                )
                df_Result_Stock = df_Result_Stock.drop(columns=[STOCK_CATEGORY])
                print(CITYPHARMACY special case processing complete.)

            # ================================
            # INTERMEDIATE SECTION CLEAN UP PRODUCT_NAME
            # ================================

            df_Result_Stock[PRODUCT_NAME] = df_Result_Stock[PRODUCT_NAME].str.replace(rs+,  )
            print(Cleaned up PRODUCT_NAME column.)

            # ================================
            # INTERMEDIATE SECTION GROUPING AND FINAL PROCESSING
            # ================================

            # Fill empty values with defaults
            df_Result_Stock[DATA_DATE].fillna(2199-12-31, inplace=True)
            df_Result_Stock[COUNTRY_NAME].fillna(NULL, inplace=True)
            df_Result_Stock[ORGANIZATION_NAME].fillna(NULL, inplace=True)
            df_Result_Stock[BRANCH_NAME].fillna(NULL, inplace=True)
            df_Result_Stock[PRODUCT_ID].fillna(NULL, inplace=True)
            df_Result_Stock[PRODUCT_NAME].fillna(NULL, inplace=True)
            df_Result_Stock[AVAILABLE_QUANTITY].fillna(0, inplace=True)
            df_Result_Stock[BLOCKED_QUANTITY].fillna(0, inplace=True)
            df_Result_Stock[INVENTORY_CATEGORY].fillna(GN, inplace=True)
            df_Result_Stock[BATCH_NUMBER].fillna(NULL, inplace=True)
            df_Result_Stock[EXPIRY_DATE].fillna(2199-12-31, inplace=True)

            # Group by relevant columns and aggregate
            df_grouped = (
                df_Result_Stock.groupby(
                    [
                        DATA_DATE,
                        COUNTRY_NAME,
                        ORGANIZATION_NAME,
                        BRANCH_NAME,
                        PRODUCT_ID,
                        PRODUCT_NAME,
                        INVENTORY_CATEGORY,
                        BATCH_NUMBER,
                        EXPIRY_DATE,
                    ]
                )
                .agg({AVAILABLE_QUANTITY sum, BLOCKED_QUANTITY sum})
                .reset_index()
            )
            print(Grouped and aggregated data.)

            # Rearrange columns in the desired order
            df_grouped = df_grouped[
                [
                    DATA_DATE,
                    COUNTRY_NAME,
                    ORGANIZATION_NAME,
                    BRANCH_NAME,
                    PRODUCT_ID,
                    PRODUCT_NAME,
                    AVAILABLE_QUANTITY,
                    BLOCKED_QUANTITY,
                    INVENTORY_CATEGORY,
                    BATCH_NUMBER,
                    EXPIRY_DATE,
                ]
            ]
            df_Result_Stock = df_grouped
            print(Rearranged columns and completed df_Result_Stock.)

            # ================================
            # SECTION END df_Result_Stock Creation Completed
            # ================================

            print(df_Result_Stock creation complete)
            print(df_Result_Stock)

        # ================================
        # SECTION SALES AND SALESSTOCK CONTEXT PROCESSING
        # ================================

        data_Date_Match_Statu = 0
        invoice_Date_Match_Statu = 0

        if (
            context in [SALES, SALESSTOCK]
            and len(base3_dataframe3)  0
            and (column_count_sales_is_mandatory_1 == column_count_matching_sales_is_mandatory_1)
            and (column_count_matching_sales_is_mandatory_1 != 0)
        )
            sales_list_column_match = list_column_match
            print(Initial Data Check)
            print(base3_dataframe3 , base3_dataframe3)

            # ================================
            # INTERMEDIATE SECTION FILTER VALID ENTRIES
            # ================================

            # Find the column name for SALES_QUANTITY in sales_list_column_match
            searched_excel_column_name_SALES_QUANTITY = next(
                (
                    item[searched_excel_column_name]
                    for item in sales_list_column_match
                    if item[etl_column_name] == SALES_QUANTITY
                ),
                None,
            )

            if not searched_excel_column_name_SALES_QUANTITY
                raise ValueError(Column 'SALES_QUANTITY' not found.)

            # Filtering exclude NaN, NaT, None, null and empty string values
            valid_entries = base3_dataframe3[
                base3_dataframe3[searched_excel_column_name_SALES_QUANTITY].apply(
                    lambda x not (pd.isna(x) or str(x).strip().lower() in [nan, nat, none, null, ])
                )
            ]

            valid_entries.reset_index(drop=True, inplace=True)
            sales_base3_dataframe3 = valid_entries.copy()

            print(Valid Entries Filtering Complete)
            print(sales_list_column_match , sales_list_column_match)

            # ================================
            # INTERMEDIATE SECTION MATCH COLUMNS AND ASSIGN VALUES
            # ================================

            matched_columns = [
                item[etl_column_name] for item in sales_list_column_match if item[etl_column_name] is not None
            ]
            print(matched_columns , matched_columns)
            print(df_Result_Sales , df_Result_Sales)
            print(sales_list_column_match , sales_list_column_match)
            print(sales_base3_dataframe3 , sales_base3_dataframe3)

            # Check for matching columns and assign values
            for column_name in df_Result_Sales.columns
                if column_name in matched_columns
                    find_searched_excel_column_name = None
                    find_etl_column_name = None

                    for item in sales_list_column_match
                        if item[etl_column_name] == column_name
                            find_searched_excel_column_name = item[searched_excel_column_name]
                            if column_name == DATA_DATE or column_name == INVOICE_DATE
                                df_Result_Sales[column_name] = pd.to_datetime(
                                    sales_base3_dataframe3[find_searched_excel_column_name]
                                )
                            elif (
                                column_name == SALES_QUANTITY
                                or column_name == RETURN_QUANTITY
                                or column_name == TAX_IDENTIFICATION_NUMBER
                            )
                                sales_base3_dataframe3[find_searched_excel_column_name].fillna(
                                    0, inplace=True
                                )
                                numeric_values = pd.to_numeric(
                                    sales_base3_dataframe3[find_searched_excel_column_name],
                                    errors=coerce,
                                )
                                df_Result_Sales[column_name] = numeric_values.fillna(0).astype(int)
                            elif column_name == SALES_VALUE or column_name == RETURN_VALUE
                                sales_base3_dataframe3[find_searched_excel_column_name].fillna(0, inplace=True)
                                numeric_values = pd.to_numeric(
                                    sales_base3_dataframe3[find_searched_excel_column_name],
                                    errors=coerce,
                                )
                                df_Result_Sales[column_name] = numeric_values.fillna(0).astype(float)
                                df_Result_Sales[column_name] = df_Result_Sales[column_name].apply(lambda x Decimal(x))
                            else
                                df_Result_Sales[column_name] = sales_base3_dataframe3[find_searched_excel_column_name]
                                break
            data_Date_Match_Statu = 0
            invoice_Date_Match_Statu = 0

            # ================================
            # INTERMEDIATE SECTION DATA DATE AND INVOICE DATE PROCESSING
            # ================================

            for column_name in df_Result_Sales.columns
                if column_name in matched_columns
                    if column_name == DATA_DATE
                        data_Date_Match_Statu = 1
                    elif column_name == INVOICE_DATE
                        invoice_Date_Match_Statu = 1
            print(1855)

            if data_Date_Match_Statu == 0 and invoice_Date_Match_Statu == 1
                df_Result_Sales[DATA_DATE] = (
                    pd.to_datetime(df_Result_Sales[INVOICE_DATE], format=%d.%m.%Y).dt.to_period(M).dt.end_time
                )
            elif data_Date_Match_Statu == 0 and invoice_Date_Match_Statu == 0
                df_Result_Sales[DATA_DATE] = data_date
                df_Result_Sales[INVOICE_DATE] = df_Result_Sales[DATA_DATE]
            elif data_Date_Match_Statu == 1 and invoice_Date_Match_Statu == 0
                df_Result_Sales[INVOICE_DATE] = df_Result_Sales[DATA_DATE]
            print(1863)

            searched_excel_column_name_DATA_DATE = next(
                (
                    item[searched_excel_column_name]
                    for item in sales_list_column_match
                    if item[etl_column_name] == DATA_DATE
                ),
                None,
            )

            # ================================
            # INTERMEDIATE SECTION DEFAULT VALUES FOR UNMATCHED COLUMNS
            # ================================

            for column_name in df_Result_Sales.columns
                if column_name not in matched_columns
                    if column_name == DATA_DATE and frequency != MONTHLY
                        df_Result_Sales[column_name] = data_date
                    elif column_name == INVOICE_DATE and frequency != MONTHLY
                        df_Result_Sales[column_name] = data_date
                    elif column_name == COUNTRY_NAME
                        df_Result_Sales[column_name] = country
                    elif column_name == ORGANIZATION_NAME
                        df_Result_Sales[column_name] = organization_name
                    elif column_name == BRANCH_NAME
                        if data_owner in [BEK, ISKOOP, SELÇUK, ALLIANCE]
                            df_Result_Sales[column_name] = sales_base3_dataframe3[apparo_branch_name]
                        else
                            df_Result_Sales[column_name] = branch_name
                    elif column_name == CUSTOMER_ID
                        df_Result_Sales[column_name] = NULL
                    elif column_name == CUSTOMER_NAME
                        df_Result_Sales[column_name] = NA
                    elif column_name == PRODUCT_ID
                        df_Result_Sales[column_name] = NULL
                    elif column_name == PRODUCT_NAME
                        df_Result_Sales[column_name] = NULL
                    elif column_name == SALES_QUANTITY
                        df_Result_Sales[column_name] = 0
                    elif column_name == RETURN_QUANTITY
                        df_Result_Sales[column_name] = 0
                    elif column_name == SALES_CATEGORY
                        df_Result_Sales[column_name] = GN
                    elif column_name == SALES_VALUE
                        df_Result_Sales[column_name] = 0
                    elif column_name == RETURN_VALUE
                        df_Result_Sales[column_name] = 0
                    elif column_name == AUCTION_NUMBER
                        df_Result_Sales[column_name] = -1
                    elif column_name == TAX_IDENTIFICATION_NUMBER
                        df_Result_Sales[column_name] = -1

            print(1902 - Default Values Assignment Complete)

            # ================================
            # INTERMEDIATE SECTION DATA ADJUSTMENTS BASED ON CONDITIONS
            # ================================

            if data_owner == RAFED and structure == CUSTOMRAFED and country == UAE
                if daq_sheet_name == Tawam Data
                    df_Result_Sales[SALES_QUANTITY] = df_Result_Sales[SALES_QUANTITY]  -1
                    df_Result_Sales[ORGANIZATION_NAME] = TAWAM
                elif daq_sheet_name == Mafraq-SSMC Data
                    df_Result_Sales[ORGANIZATION_NAME] = SSMC

            print(Data Adjustments Complete)

            if data_owner == CITYPHARMACY and context == SALES and country == UAE
                df_Result_Sales[SALES_CATEGORY] = df_Result_Sales.apply(
                    lambda row (
                        PU
                        if data_owner == CITYPHARMACY and row[SALES_CATEGORY] == I
                        else (PR if data_owner == CITYPHARMACY and row[SALES_CATEGORY] == N else GN)
                    ),
                    axis=1,
                )

                searched_excel_column_name_REGION = next(
                    (
                        item[searched_excel_column_name]
                        for item in sales_list_column_match
                        if item[etl_column_name] == REGION
                    ),
                    None,
                )

                df_Result_Sales[REGION_NAME] = sales_base3_dataframe3[searched_excel_column_name_REGION]
                df_Result_Sales[REGION_NAME] = df_Result_Sales[REGION_NAME].str.upper()
                df_Result_Sales[REGION_NAME] = df_Result_Sales[REGION_NAME].str.replace(
                    İ, I
                )

                df_Result_Sales[BRANCH_NAME] = df_Result_Sales.apply(
                    lambda row (
                        row[ORGANIZATION_NAME] +  KIZAD
                        if pd.notna(row[REGION_NAME])
                        and row[COUNTRY_NAME] == UAE
                        and row[ORGANIZATION_NAME] == CITYPHARMACY
                        and row[REGION_NAME].upper() in [ABU DHABI, AL AIN]
                        else (
                            row[ORGANIZATION_NAME] +  SHARJAH
                            if pd.notna(row[REGION_NAME])
                            and row[COUNTRY_NAME] == UAE
                            and row[ORGANIZATION_NAME] == CITYPHARMACY
                            and row[REGION_NAME].upper() not in [ABU DHABI, AL AIN]
                            else (
                                row[ORGANIZATION_NAME] +  SHARJAH
                                if pd.isna(row[REGION_NAME])
                                and row[COUNTRY_NAME] == UAE
                                and row[ORGANIZATION_NAME] == CITYPHARMACY
                                else row[BRANCH_NAME]
                            )
                        )
                    ),
                    axis=1,
                )

                df_Result_Sales = df_Result_Sales.drop(columns=[REGION_NAME])

            print(Citypharmacy Adjustments Complete)

            # ================================
            # INTERMEDIATE SECTION ADJUST SALES AND RETURN VALUES
            # ================================

            df_Result_Sales[RETURN_QUANTITY] = df_Result_Sales[SALES_QUANTITY].apply(
                lambda x abs(x) if x  0 else 0
            )

            df_Result_Sales[RETURN_VALUE] = np.where(
                df_Result_Sales[SALES_QUANTITY]  0,
                abs(df_Result_Sales[SALES_VALUE]),
                0,
            )

            df_Result_Sales.loc[df_Result_Sales[SALES_QUANTITY]  0, SALES_VALUE] = 0
            df_Result_Sales[SALES_QUANTITY] = df_Result_Sales[SALES_QUANTITY].apply(lambda x max(0, x))

            print(1942 - Sales and Return Values Adjustment Complete)

            # ================================
            # INTERMEDIATE SECTION CLEANUP AND GROUPING
            # ================================

            df_Result_Sales[PRODUCT_NAME] = df_Result_Sales[PRODUCT_NAME].str.replace(rs+,  )

            df_Result_Sales[DATA_DATE].fillna(2199-12-31, inplace=True)
            df_Result_Sales[INVOICE_DATE].fillna(2199-12-31, inplace=True)
            df_Result_Sales[COUNTRY_NAME].fillna(NULL, inplace=True)
            df_Result_Sales[ORGANIZATION_NAME].fillna(NULL, inplace=True)
            df_Result_Sales[BRANCH_NAME].fillna(NULL, inplace=True)
            df_Result_Sales[CUSTOMER_ID].fillna(NULL, inplace=True)
            df_Result_Sales[CUSTOMER_NAME].fillna(NA, inplace=True)
            df_Result_Sales[PRODUCT_ID].fillna(NULL, inplace=True)
            df_Result_Sales[PRODUCT_NAME].fillna(NULL, inplace=True)
            df_Result_Sales[SALES_QUANTITY].fillna(0, inplace=True)
            df_Result_Sales[RETURN_QUANTITY].fillna(0, inplace=True)
            df_Result_Sales[SALES_CATEGORY].fillna(GN, inplace=True)
            df_Result_Sales[SALES_VALUE].fillna(0, inplace=True)
            df_Result_Sales[RETURN_VALUE].fillna(0, inplace=True)
            df_Result_Sales[AUCTION_NUMBER].fillna(-1, inplace=True)
            df_Result_Sales[TAX_IDENTIFICATION_NUMBER].fillna(-1, inplace=True)

            df_grouped = (
                df_Result_Sales.groupby(
                    [
                        DATA_DATE,
                        COUNTRY_NAME,
                        ORGANIZATION_NAME,
                        BRANCH_NAME,
                        CUSTOMER_ID,
                        CUSTOMER_NAME,
                        PRODUCT_ID,
                        PRODUCT_NAME,
                        INVOICE_DATE,
                        SALES_CATEGORY,
                        AUCTION_NUMBER,
                        TAX_IDENTIFICATION_NUMBER,
                    ]
                )
                .agg(
                    {
                        SALES_QUANTITY sum,
                        RETURN_QUANTITY sum,
                        SALES_VALUE sum,
                        RETURN_VALUE sum,
                    }
                )
                .reset_index()
            )

            # Rearrange column order
            df_grouped = df_grouped[
                [
                    DATA_DATE,
                    COUNTRY_NAME,
                    ORGANIZATION_NAME,
                    BRANCH_NAME,
                    CUSTOMER_ID,
                    CUSTOMER_NAME,
                    PRODUCT_ID,
                    PRODUCT_NAME,
                    INVOICE_DATE,
                    SALES_QUANTITY,
                    RETURN_QUANTITY,
                    SALES_CATEGORY,
                    SALES_VALUE,
                    RETURN_VALUE,
                    AUCTION_NUMBER,
                    TAX_IDENTIFICATION_NUMBER,
                ]
            ]

            df_Result_Sales = df_grouped
            print(df_Result_Sales - Final Output)
            print(df_Result_Sales , df_Result_Sales)

        # ================================
        # START OF INSERT AND UPDATE OPERATIONS FOR POSTGRESQL DATABASE TABLES
        # ================================

        if df_Result_Stock.shape[0]  0
            # ================================
            # INTERMEDIATE SECTION STOCK DATA FRAME TO POSTGRESQL
            # ================================

            # Create Spark session
            spark = SparkSession.builder.appName(PostgreSQL Writing).getOrCreate()

            # Iterate through column names and rename them to lowercase
            df_Result_Stock.columns = [column_name.lower() for column_name in df_Result_Stock.columns]

            # Print the original and new column names
            for column_name in df_Result_Stock.columns
                print(fNew column name {column_name})

            # Define schema for Spark DataFrame with lowercase column names
            schema = StructType(
                [
                    StructField(data_date, DateType(), nullable=True),
                    StructField(country_name, StringType(), nullable=True),
                    StructField(organization_name, StringType(), nullable=True),
                    StructField(branch_name, StringType(), nullable=True),
                    StructField(product_id, StringType(), nullable=True),
                    StructField(product_name, StringType(), nullable=True),
                    StructField(available_quantity, IntegerType(), nullable=True),
                    StructField(blocked_quantity, IntegerType(), nullable=True),
                    StructField(inventory_category, StringType(), nullable=True),
                    StructField(batch_number, StringType(), nullable=True),
                    StructField(expiry_date, DateType(), nullable=True),
                ]
            )

            # Create Spark DataFrame from Pandas DataFrame with the defined schema
            spark_df = spark.createDataFrame(df_Result_Stock, schema=schema)

            # Write Spark DataFrame to PostgreSQL using JDBC
            spark_df.write.format(jdbc).option(url, jdbc_url).option(
                dbtable, f{schemaname}.{table_name}_i
            ).option(user, username).option(password, password).mode(overwrite).save()

            print(Stock DataFrame written to PostgreSQL.)

            # ================================
            # INTERMEDIATE SECTION CALL POSTGRESQL PROCEDURE
            # ================================

            try
                # Create PostgreSQL JDBC connection
                connSpark = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
                stmt = connSpark.createStatement()
                sql_execute_query = (
                    fCALL {schemaname}.proc_update_temp_table('{schemaname}', '{table_name}_i')
                )
                print(Executing SQL procedure, sql_execute_query)
                stmt.execute(sql_execute_query)
                print(Procedure successfully called.)

            except Exception as e
                print(Error, e)

            finally
                if connSpark
                    connSpark.close()

            # Update temporary load info
            insert_into_temp_load_info(table_name + _i, 'INVENTORY', frequency, 0, 0)

        if df_Result_Sales.shape[0]  0
            # ================================
            # INTERMEDIATE SECTION SALES DATA FRAME TO POSTGRESQL
            # ================================

            # Create Spark session
            spark = SparkSession.builder.appName(Write to PostgreSQL).getOrCreate()

            # Iterate through column names and rename them to lowercase
            df_Result_Sales.columns = [column_name.lower() for column_name in df_Result_Sales.columns]

            # Print the original and new column names
            for column_name in df_Result_Sales.columns
                print(fNew column name {column_name})

            # Define schema for Spark DataFrame with lowercase column names
            schema = StructType(
                [
                    StructField(data_date, DateType(), nullable=True),
                    StructField(country_name, StringType(), nullable=True),
                    StructField(organization_name, StringType(), nullable=True),
                    StructField(branch_name, StringType(), nullable=True),
                    StructField(customer_id, StringType(), nullable=True),
                    StructField(customer_name, StringType(), nullable=True),
                    StructField(product_id, StringType(), nullable=True),
                    StructField(product_name, StringType(), nullable=True),
                    StructField(invoice_date, DateType(), nullable=True),
                    StructField(sales_quantity, LongType(), nullable=True),
                    StructField(return_quantity, LongType(), nullable=True),
                    StructField(sales_category, StringType(), nullable=True),
                    StructField(sales_value, DecimalType(22, 2), nullable=True),
                    StructField(return_value, DecimalType(22, 2), nullable=True),
                    StructField(auction_number, StringType(), nullable=True),
                    StructField(tax_identification_number, LongType(), nullable=True),
                ]
            )

            # Convert SALES_VALUE and RETURN_VALUE to Decimal type
            df_Result_Sales[sales_value] = df_Result_Sales[sales_value].apply(lambda x Decimal(x))
            df_Result_Sales[return_value] = df_Result_Sales[return_value].apply(lambda x Decimal(x))

            # Create Spark DataFrame from Pandas DataFrame with the defined schema
            spark_df = spark.createDataFrame(df_Result_Sales, schema=schema)
            print(Sales DataFrame schema, schema)
            print(Writing Sales DataFrame to PostgreSQL.)

            # Write Spark DataFrame to PostgreSQL using JDBC
            spark_df.write.format(jdbc).option(url, jdbc_url).option(
                dbtable, f{schemaname}.{table_name}_s
            ).option(user, username).option(password, password).mode(overwrite).save()

            print(Sales DataFrame written to PostgreSQL.)

            # ================================
            # INTERMEDIATE SECTION CALL POSTGRESQL PROCEDURE
            # ================================

            try
                # Create PostgreSQL JDBC connection
                connSpark = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
                stmt = connSpark.createStatement()
                sql_execute_query = (
                    fCALL {schemaname}.proc_update_temp_table('{schemaname}', '{table_name}_s')
                )
                print(Executing SQL procedure, sql_execute_query)
                stmt.execute(sql_execute_query)
                print(Procedure successfully called.)

            except Exception as e
                print(Error, e)

            finally
                if connSpark
                    connSpark.close()

            # Update temporary load info
            insert_into_temp_load_info(table_name + _s, 'SALES', frequency, invoice_Date_Match_Statu, 0)

        # ================================
        # INTERMEDIATE SECTION FINAL UPDATE TO DAQ_LOG_INFO
        # ================================

        update_daq_log_info(id)

# =======================================================
# FINAL STEP Mark All Unprocessed, Non-Corrupt Records as Processed in DAQ_LOG_INFO
# =======================================================

update_daq_log_info('ALL_RECORDS')

# ============================================================
# Job Commit
# ============================================================
print(Step Final Committing the Glue job.)
glueJob.commit()
