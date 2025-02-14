"""
Configuration module for ETL job settings and queries.
"""

from .settings import (
    AWS_REGION,
    S3_BUCKET_PREFIX,
    DB_CONFIG,
    DATE_FORMATS,
    SPECIAL_CASES,
    STOCK_SCHEMA,
    SALES_SCHEMA
)
from .queries import (
    DAQ_LOG_INFO_QUERY,
    ENTITY_DETAIL_QUERY_WITH_SHEET,
    ENTITY_DETAIL_QUERY_WITH_FILE,
    ATTRIBUTE_DETAIL_QUERY,
    UPDATE_DAQ_LOG_INFO
)

__all__ = [
    'AWS_REGION',
    'S3_BUCKET_PREFIX',
    'DB_CONFIG',
    'DATE_FORMATS',
    'SPECIAL_CASES',
    'STOCK_SCHEMA',
    'SALES_SCHEMA',
    'DAQ_LOG_INFO_QUERY',
    'ENTITY_DETAIL_QUERY_WITH_SHEET',
    'ENTITY_DETAIL_QUERY_WITH_FILE',
    'ATTRIBUTE_DETAIL_QUERY',
    'UPDATE_DAQ_LOG_INFO'
]
