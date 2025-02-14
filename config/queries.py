"""
SQL query templates for the ETL job.
"""

# Query to get data from DAQ_LOG_INFO table
DAQ_LOG_INFO_QUERY = """
SELECT DISTINCT 
    id, 
    receiver_address, 
    sender_address, 
    file, 
    sheet_name as daq_sheet_name, 
    mail_date,
    CASE 
        WHEN position('.' in reverse(file)) > 0 THEN 
            substring(file from (char_length(file) - position('.' in reverse(file)) + 2))
        ELSE 'EMPTY'
    END as file_extension,
    (date_trunc('month', mail_date) - INTERVAL '1 day')::date AS mail_date_prev_month_last_day
FROM {schema}.daq_log_info a
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
        FROM {schema}.daq_log_info
        WHERE is_corrupt = 0 
        AND is_processed = 0
        GROUP BY 
            file,
            sheet_name,
            subject,
            receiver_address,
            sender_address,
            to_char(mail_date, 'yyyymmdd')
    ) b
    WHERE a.id = b.max_id
)
"""

# Query to get data from DD_ENTITY_DETAIL table with sheet name filter
ENTITY_DETAIL_QUERY_WITH_SHEET = """
SELECT DISTINCT 
    data_owner, 
    country, 
    context, 
    period, 
    frequency, 
    data_owner_mail, 
    structure,
    file_table_name as entity_file_table_name, 
    sheet_name as entity_sheet_name,
    lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1,
    lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g')) as sheet_name2,
    CASE 
        WHEN lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
             lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g'))
        THEN 1
        ELSE 0
    END as sheet_name_comparison_result
FROM {schema}.dd_entity_detail
WHERE is_api = 0
AND data_owner_mail = '{sender_address}'
AND lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
    lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g'))
"""

# Query to get data from DD_ENTITY_DETAIL table with file extension filter
ENTITY_DETAIL_QUERY_WITH_FILE = """
SELECT DISTINCT 
    data_owner, 
    country, 
    context, 
    period, 
    frequency, 
    data_owner_mail, 
    structure,
    file_table_name as entity_file_table_name, 
    sheet_name as entity_sheet_name,
    lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1,
    lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g')) as sheet_name2,
    CASE 
        WHEN lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
             lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g'))
        THEN 1
        ELSE 0
    END as sheet_name_comparison_result
FROM (
    SELECT ed.*,
        CASE  
            WHEN position('.' in reverse(file_table_name)) > 0
            THEN substring(file_table_name from (char_length(file_table_name) - position('.' in reverse(file_table_name)) + 2))
            ELSE 'EMPTY'
        END as file_extension
    FROM {schema}.dd_entity_detail ed
)
WHERE is_api = 0
AND CASE 
        WHEN upper(coalesce(replace(file_extension, 'n', ''), 'file_ext')) = 'XLS'
        THEN 'XLSX'
        ELSE upper(coalesce(replace(file_extension, 'n', ''), 'file_ext'))
    END =
    CASE 
        WHEN replace(upper(coalesce(replace('{file_extension}', 'n', ''), 'file_ext')), 'İ', 'I') = 'XLS'
        THEN 'XLSX'
        ELSE replace(upper(coalesce(replace('{file_extension}', 'n', ''), 'file_ext')), 'İ', 'I')
    END
"""

# Query to get data from DD_ATTRIBUTE_DETAIL table
ATTRIBUTE_DETAIL_QUERY = """
SELECT 
    original_column_name, 
    second_column_name, 
    etl_column_name,
    column_position, 
    starting_row, 
    is_mandatory
FROM {schema}.dd_attribute_detail
WHERE data_owner = '{data_owner}'
AND context = '{context}'
AND file_table_name = '{file_table_name}'
AND sheet_name = '{sheet_name}'
ORDER BY column_position, starting_row
"""

# Query to update DAQ_LOG_INFO table
UPDATE_DAQ_LOG_INFO = """
UPDATE {schema}.daq_log_info 
SET is_processed = 1 
WHERE {condition}
"""

# Query without sheet name filter
ENTITY_DETAIL_QUERY_NO_SHEET = """
SELECT DISTINCT 
    data_owner, country, context, period, frequency, data_owner_mail, structure,
    file_table_name as entity_file_table_name, sheet_name as entity_sheet_name,
    lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1,
    lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g')) as sheet_name2,
    CASE WHEN lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
              lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g'))
         THEN 1 ELSE 0 
    END as sheet_name_comparison_result
FROM {schema}.dd_entity_detail
WHERE is_api = 0
AND data_owner_mail = '{sender_address}'
"""

# Query with country and filename matching
ENTITY_DETAIL_QUERY_COUNTRY = """
SELECT DISTINCT 
    data_owner, 
    country, 
    context, 
    period, 
    frequency, 
    data_owner_mail, 
    structure,
    file_table_name as entity_file_table_name, 
    sheet_name as entity_sheet_name,
    lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) as sheet_name1,
    lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g')) as sheet_name2,
    CASE 
        WHEN lower(regexp_replace(sheet_name, '[0-9]', '', 'g')) = 
             lower(regexp_replace('{daq_sheet_name}', '[0-9]', '', 'g'))
        THEN 1 
        ELSE 0 
    END as sheet_name_comparison_result
FROM (
    SELECT ed.*,
        CASE  
            WHEN position('.' in reverse(file_table_name)) > 0
            THEN substring(file_table_name from (char_length(file_table_name) - position('.' in reverse(file_table_name)) + 2))
            ELSE 'EMPTY'
        END as file_extension
    FROM {schema}.dd_entity_detail ed
)
WHERE is_api = 0
AND CASE 
        WHEN upper(coalesce(replace(file_extension, 'n', ''), 'file_ext')) = 'XLS'
        THEN 'XLSX'
        ELSE upper(coalesce(replace(file_extension, 'n', ''), 'file_ext'))
    END =
    CASE 
        WHEN replace(upper(coalesce(replace('{file_extension}', 'n', ''), 'file_ext')), 'İ', 'I') = 'XLS'
        THEN 'XLSX'
        ELSE replace(upper(coalesce(replace('{file_extension}', 'n', ''), 'file_ext')), 'İ', 'I')
    END
AND (
    (
        (replace(replace(Upper(data_owner), 'İ', 'I'), ' ', '') = replace(replace(Upper('{sender_address}'), 'İ', 'I'), ' ', ''))
        OR
        ((SELECT (Regexp_matches(data_owner_mail, '@([^.]+)'))[1]) = '{sender_address}')
    )
    OR
    (
        country = (
            SELECT DISTINCT dc.country_name
            FROM {schema}.email_connection_info eci,
                 {schema}.dim_countries dc
            WHERE eci.email_address = '{receiver_address}'
            AND dc.country_id = eci.country_id
        )
        AND (
            CASE
                WHEN POSITION('-' IN file_table_name) > 0
                THEN SUBSTRING(file_table_name, 1, POSITION('-' IN file_table_name) - 1)
                WHEN POSITION('.' IN file_table_name) > 0
                THEN SUBSTRING(file_table_name, 1, POSITION('.' IN file_table_name) - 1)
                ELSE file_table_name
            END
        ) = (
            SELECT DISTINCT
                CASE
                    WHEN POSITION('-' IN file) > 0 THEN SUBSTRING(file, 1, POSITION('-' IN file) - 1)
                    WHEN POSITION('.' IN file) > 0 THEN SUBSTRING(file, 1, POSITION('.' IN file) - 1)
                    ELSE file
                END AS file
            FROM {schema}.daq_log_info
            WHERE id = {file_id}
        )
    )
)
"""
