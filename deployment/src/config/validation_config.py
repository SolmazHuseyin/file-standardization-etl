"""
Configuration module for data validation rules and constants.
"""

from typing import Dict, List, Any
from datetime import datetime, date

# Stock Data Validation Rules
STOCK_VALIDATION_RULES = {
    'required_columns': [
        'DATA_DATE',
        'COUNTRY_NAME',
        'ORGANIZATION_NAME',
        'PRODUCT_ID',
        'AVAILABLE_QUANTITY'
    ],
    'numeric_columns': [
        'AVAILABLE_QUANTITY',
        'BLOCKED_QUANTITY'
    ],
    'date_columns': [
        'DATA_DATE',
        'EXPIRY_DATE'
    ],
    'categorical_columns': {
        'INVENTORY_CATEGORY': ['GN', 'PR', 'PU']
    },
    'min_date': date(2020, 1, 1),
    'max_date': datetime.now().date()
}

# Sales Data Validation Rules
SALES_VALIDATION_RULES = {
    'required_columns': [
        'DATA_DATE',
        'COUNTRY_NAME',
        'ORGANIZATION_NAME',
        'PRODUCT_ID',
        'SALES_QUANTITY',
        'SALES_VALUE'
    ],
    'numeric_columns': [
        'SALES_QUANTITY',
        'RETURN_QUANTITY',
        'SALES_VALUE',
        'RETURN_VALUE',
        'TAX_IDENTIFICATION_NUMBER'
    ],
    'date_columns': [
        'DATA_DATE',
        'INVOICE_DATE'
    ],
    'categorical_columns': {
        'SALES_CATEGORY': ['GN', 'PR', 'PU']
    },
    'min_date': date(2020, 1, 1),
    'max_date': datetime.now().date()
}

# Organization-specific Rules
ORGANIZATION_RULES = {
    'CITYPHARMACY': {
        'required_branch_name': True,
        'allowed_categories': ['GN', 'PR', 'PU'],
        'country_specific_rules': {
            'KUWAIT': {
                'split_tilde_rows': True
            }
        }
    },
    'RAFED': {
        'sheet_name_mapping': {
            'Stock': 'RAFED',
            'Sales': 'RAFED'
        }
    },
    'ALLIANCE': {
        'country_specific_rules': {
            'KUWAIT': {
                'process_branch_name': True
            }
        }
    }
}

# Data Type Mappings
COLUMN_DATA_TYPES = {
    'DATA_DATE': 'datetime64[ns]',
    'COUNTRY_NAME': 'str',
    'ORGANIZATION_NAME': 'str',
    'PRODUCT_ID': 'str',
    'AVAILABLE_QUANTITY': 'float64',
    'BLOCKED_QUANTITY': 'float64',
    'SALES_QUANTITY': 'float64',
    'RETURN_QUANTITY': 'float64',
    'SALES_VALUE': 'float64',
    'RETURN_VALUE': 'float64',
    'TAX_IDENTIFICATION_NUMBER': 'str'
}

# Error Messages
ERROR_MESSAGES = {
    'missing_columns': "Missing required columns: {columns}",
    'invalid_numeric': "Non-numeric values found in {column}: {values}",
    'invalid_date': "Invalid dates found in {column}: {values}",
    'invalid_category': "Invalid values in {column}: {values}",
    'negative_quantity': "Negative values found in {column}",
    'return_exceeds_sales': "Return {type} exceeds sales {type}",
    'product_mismatch': "Products in sales but not in stock: {products}",
    'org_mismatch': "Organizations not matching between stock and sales: {orgs}"
} 