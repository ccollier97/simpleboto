# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""


class C:
    """
    Constants class for Athena related constants.
    """
    # Allowed Parameters for Schema
    DATABASE_NAME = 'DATABASE_NAME'
    TABLE_NAME = 'TABLE_NAME'
    S3_BUCKET = 'S3_BUCKET'
    S3_PREFIX = 'S3_PREFIX'
    FILE_FORMAT = 'FILE_FORMAT'
    FILE_COMPRESSION = 'FILE_COMPRESSION'
    SKIP_HEADER = 'SKIP_HEADER'
    PARTITION_SCHEMA = 'PARTITION_SCHEMA'
    PARTITION_PROJECTION = 'PARTITION_PROJECTION'

    # Miscellaneous
    PARQUET_ = 'parquet'
    CSV_ = 'csv'

    SNAPPY_ = 'snappy'
    GZIP_ = 'gzip'
