"""
Configuration package for the e-commerce lakehouse architecture.

This package contains configuration settings for the e-commerce lakehouse architecture.
"""

from config.settings import (
    AWS_REGION,
    S3_BUCKET_NAME,
    S3_PREFIX_STRUCTURE,
    S3_PREFIXES,
    DATA_FILES,
    BATCH_SIZE,
    PROCESSING_TIMEOUT,
    LOG_LEVEL,
    LOG_FORMAT,
    GLUE_DATABASE_PREFIX,
    GLUE_DATABASES,
    DELTA_TABLE_PROPERTIES,
    SCHEMA_VALIDATION,
    get_prefix,
    get_all_settings,
)

__all__ = [
    "AWS_REGION",
    "S3_BUCKET_NAME",
    "S3_PREFIX_STRUCTURE",
    "S3_PREFIXES",
    "DATA_FILES",
    "BATCH_SIZE",
    "PROCESSING_TIMEOUT",
    "LOG_LEVEL",
    "LOG_FORMAT",
    "GLUE_DATABASE_PREFIX",
    "GLUE_DATABASES",
    "DELTA_TABLE_PROPERTIES",
    "SCHEMA_VALIDATION",
    "get_prefix",
    "get_all_settings",
]
