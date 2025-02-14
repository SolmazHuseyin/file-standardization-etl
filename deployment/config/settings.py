"""
Configuration settings for the ETL job.
"""

# AWS Settings
AWS_REGION = 'eu-central-1'
S3_BUCKET_PREFIX = 's3://pt-s3-imip-{env}-imip-all-data/mail/mnt/c/Roche/IMIP-file/RECEIVED_FILE_PATH'

# Database Settings
DB_CONFIG = {
    'schema': 'imip',
    'driver': 'org.postgresql.Driver',
}

# Date Formats
DATE_FORMATS = [
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

    # 2-digit year formats (YY)
    "%d-%m-%y %H%M%S",  # 31-12-24 235959
    "%d-%m-%y %H%M",    # 31-12-24 2359
    "%d-%m-%y",         # 31-12-24
    "%y-%m-%d %H%M%S",  # 24-12-31 235959
    "%y-%m-%d %H%M",    # 24-12-31 2359
    "%y-%m-%d",         # 24-12-31
    "%m-%d-%y %H%M%S",  # 12-31-24 235959
    "%m-%d-%y %H%M",    # 12-31-24 2359
    "%m-%d-%y",         # 12-31-24

    # Dot-separated 2-digit year
    "%d.%m.%y %H%M%S",  # 31.12.24 235959
    "%d.%m.%y %H%M",    # 31.12.24 2359
    "%d.%m.%y",         # 31.12.24
    "%y.%m.%d %H%M%S",  # 24.12.31 235959
    "%y.%m.%d %H%M",    # 24.12.31 2359
    "%y.%m.%d",         # 24.12.31
    "%m.%d.%y %H%M%S",  # 12.31.24 235959
    "%m.%d.%y %H%M",    # 12.31.24 2359
    "%m.%d.%y",         # 12.31.24

    # No separator 2-digit year
    "%y%m%d%H%M%S",     # 241231235959
    "%y%m%d%H%M",       # 2412312359
    "%y%m%d",           # 241231
]

# Special Cases Configuration
SPECIAL_CASES = {
    'ALLIANCE': {
        'country': 'TURKIYE',
        'sheet_name': 'urundepobazinda'
    },
    'RAFED': {
        'country': 'UAE',
        'valid_sheets': ['mafraq-ssmc data', 'tawam data'],
        'SHEET_MAPPING': {
            'TAWAM DATA': 'TAWAM',
            'MAFRAQ-SSMC DATA': 'SSMC'
        }
    },
    'CITYPHARMACY': {
        'STOCK_CATEGORIES': {'I': 'PU', 'N': 'PR'},
        'BRANCH_MAPPING': {
            'ABU DHABI': 'KIZAD',
            'AL AIN': 'KIZAD',
            'DEFAULT': 'SHARJAH'
        }
    }
}

# DataFrame Schemas
STOCK_SCHEMA = {
    'DATA_DATE': 'datetime64[ns]',
    'COUNTRY_NAME': 'object',
    'ORGANIZATION_NAME': 'object',
    'BRANCH_NAME': 'object',
    'PRODUCT_ID': 'object',
    'PRODUCT_NAME': 'object',
    'AVAILABLE_QUANTITY': 'Int64',
    'BLOCKED_QUANTITY': 'Int64',
    'INVENTORY_CATEGORY': 'object',
    'BATCH_NUMBER': 'object',
    'EXPIRY_DATE': 'datetime64[ns]'
}

SALES_SCHEMA = {
    'DATA_DATE': 'datetime64[ns]',
    'COUNTRY_NAME': 'object',
    'ORGANIZATION_NAME': 'object',
    'BRANCH_NAME': 'object',
    'CUSTOMER_ID': 'object',
    'CUSTOMER_NAME': 'object',
    'PRODUCT_ID': 'object',
    'PRODUCT_NAME': 'object',
    'INVOICE_DATE': 'datetime64[ns]',
    'SALES_QUANTITY': 'Int64',
    'RETURN_QUANTITY': 'Int64',
    'SALES_CATEGORY': 'object',
    'SALES_VALUE': 'float64',
    'RETURN_VALUE': 'float64',
    'AUCTION_NUMBER': 'object',
    'TAX_IDENTIFICATION_NUMBER': 'Int64'
}

COLUMN_MAPPINGS = {
    'QUANTITY_AVAILABLE': 'AVAILABLE_QUANTITY',
    'BATCH_NO': 'BATCH_NUMBER',
    'REGION': 'BRANCH_NAME'  # For specific cases
}

SPECIAL_CASE_MAPPINGS = {
    'SURGIPHARM': {
        'POSITION': {
            'column_mappings': {
                'REGION': 'BRANCH_NAME'
            }
        }
    }
}

# Data Owner Constants
DATA_OWNERS = {
    'ALLIANCE': 'ALLIANCE',
    'RAFED': 'RAFED',
    'CITYPHARMACY': 'CITYPHARMACY',
    'SURGIPHARM': 'SURGIPHARM',
    'QUIMICA_SUIZA': 'QUIMICA SUIZA',
    'BEK': 'BEK',
    'ISKOOP': 'ISKOOP',
    'SELCUK': 'SELÇUK',
    'SIMGE_ECZA': 'SİMGE ECZA DEPOSU MERKEZ',
    'YUSUFPASA': 'YUSUFPAŞA',
    'NEVZAT': 'NEVZAT',
    'VEGA': 'VEGA'
}

# Country Constants
COUNTRIES = {
    'PERU': 'PERU',
    'UAE': 'UAE',
    'TURKIYE': 'TURKIYE',
    'KENYA': 'KENYA',
    'SERBIA': 'SERBIA'
}

# Structure Types
STRUCTURE_TYPES = {
    'TABULAR': 'TABULAR',
    'POSITION': 'POSITION',
    'CUSTOM_POSITION': 'CUSTOM POSITION',
    'CUSTOMRAFED': 'CUSTOMRAFED'
}

# Context Types
CONTEXT_TYPES = {
    'SALES': 'SALES',
    'STOCK': 'STOCK',
    'SALESSTOCK': 'SALESSTOCK'
}

# Frequency Types
FREQUENCY_TYPES = {
    'MONTHLY': 'MONTHLY',
    'WEEKLY': 'WEEKLY',
    'DAILY': 'DAILY'
}

# Column Names
COLUMN_NAMES = {
    'DATA_DATE': 'DATA_DATE',
    'COUNTRY_NAME': 'COUNTRY_NAME',
    'ORGANIZATION_NAME': 'ORGANIZATION_NAME',
    'BRANCH_NAME': 'BRANCH_NAME',
    'PRODUCT_ID': 'PRODUCT_ID',
    'PRODUCT_NAME': 'PRODUCT_NAME',
    'AVAILABLE_QUANTITY': 'AVAILABLE_QUANTITY',
    'BLOCKED_QUANTITY': 'BLOCKED_QUANTITY',
    'INVENTORY_CATEGORY': 'INVENTORY_CATEGORY',
    'BATCH_NUMBER': 'BATCH_NUMBER',
    'EXPIRY_DATE': 'EXPIRY_DATE',
    'CUSTOMER_ID': 'CUSTOMER_ID',
    'CUSTOMER_NAME': 'CUSTOMER_NAME',
    'INVOICE_DATE': 'INVOICE_DATE',
    'SALES_QUANTITY': 'SALES_QUANTITY',
    'RETURN_QUANTITY': 'RETURN_QUANTITY',
    'SALES_CATEGORY': 'SALES_CATEGORY',
    'SALES_VALUE': 'SALES_VALUE',
    'RETURN_VALUE': 'RETURN_VALUE',
    'AUCTION_NUMBER': 'AUCTION_NUMBER',
    'TAX_IDENTIFICATION_NUMBER': 'TAX_IDENTIFICATION_NUMBER',
    'REGION': 'REGION',
    'REGION_NAME': 'REGION_NAME',
    'STOCK_CATEGORY': 'STOCK_CATEGORY',
    'QUANTITY_IN_TRANSIT1': 'QUANTITY_IN_TRANSIT1',
    'QUANTITY_IN_TRANSIT2': 'QUANTITY_IN_TRANSIT2',
    'BRANCH_CODE': 'BRANCH_CODE'
}

# Special Values
SPECIAL_VALUES = {
    'NULL': 'NULL',
    'NA': 'NA',
    'GN': 'GN',
    'PR': 'PR',
    'PU': 'PU',
    'I': 'I',
    'N': 'N'
}

# Location Constants
LOCATION_NAMES = {
    'SHARJAH': 'SHARJAH',
    'KIZAD': 'KIZAD',
    'ABU_DHABI': 'ABU DHABI',
    'AL_AIN': 'AL AIN',
    'TAWAM': 'TAWAM',
    'SSMC': 'SSMC',
    'NAIROBI': 'NAIROBI'
}

# Sheet Names
SHEET_NAMES = {
    'TAWAM_DATA': 'Tawam Data',
    'MAFRAQ_SSMC_DATA': 'Mafraq-SSMC Data',
    'URUNDEPOBAZINDA': 'urundepobazinda',
    'SHEET1': 'Sheet1'
}

# Branch Name Templates
BRANCH_TEMPLATES = {
    'SURGIPHARM_NAIROBI': 'SURGIPHARM NAIROBI',
    'QUIMICA_SUIZA_PUBLIC': 'QUIMICA SUIZA PUBLIC',
    'QUIMICA_SUIZA_PRIVATE': 'QUIMICA SUIZA PRIVATE'
}

# Database Constants
DB_CONSTANTS = {
    'JDBC': 'jdbc',
    'OVERWRITE': 'overwrite',
    'APPEND': 'append'
}

# Null Value Indicators
NULL_VALUES = ['', 'nan', 'nat', 'null', 'NULL', 'None']
