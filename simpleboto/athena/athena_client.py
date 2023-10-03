# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import os
from typing import Optional, Dict

import boto3

from simpleboto.athena.constants import C
from simpleboto.athena.utils.schema import Schema, SchemaType
from simpleboto.boto3_base import Boto3Base
from simpleboto.exceptions import (
    UnexpectedParameterError,
    NoParameterError
)
from simpleboto.s3.s3_url import S3Url
from simpleboto.utils import get_file


class AthenaClient(Boto3Base):
    """
    Wrapper for the boto3 Athena client.
    """
    SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql')

    def __init__(
        self,
        region_name: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None
    ) -> None:
        """
        :param region_name: the name of the AWS region (if not provided, ensure credentials have been exported)
        :param boto3_session: a provided boto3_session
        """
        super().__init__('athena', region_name, boto3_session)
        self.athena = self.client

    @classmethod
    def get_create_table(
        cls,
        schema: Schema
    ) -> str:
        """
        Function to return a CREATE TABLE query based on the input Schema.

        :param schema: the Schema class used to generate the information needed for the query
        """
        metadata = schema.metadata
        cls.validate_metadata(metadata)

        sql_template = get_file(location=os.path.join(cls.SQL_DIR, 'create_table.sql'))

        column_schema = cls.get_column_schema(schema.raw)
        row_format = cls.get_storage_info(metadata)
        location = cls.get_s3_location(metadata)
        partitioned_by = cls.get_partition_info(metadata)
        tbl_properties = cls.get_tbl_properties(metadata)

        return sql_template.format(
            database_name=f'{metadata.get(C.DATABASE_NAME).lower()}.' if C.DATABASE_NAME in metadata else '',
            table_name=metadata.get(C.TABLE_NAME).lower(),
            column_schema=column_schema,
            partitioned_by=partitioned_by,
            row_format_serde=row_format,
            location=location,
            tbl_properties=tbl_properties
        )

    @staticmethod
    def validate_metadata(
        metadata: dict
    ) -> None:
        """
        Function to validate the metadata from the Schema for Athena.
        This checks it has the required columns and so on.

        :param metadata: the dictionary of metadata values
        """
        req_fields = [
            C.TABLE_NAME,
            C.S3_BUCKET,
            C.S3_PREFIX,
            C.FILE_FORMAT,
            C.FILE_COMPRESSION
        ]
        given_keys = metadata.keys()
        missing_keys = [k for k in req_fields if k not in given_keys]

        if missing_keys:
            raise NoParameterError('Schema metadata', req_param=missing_keys)

        if C.PARTITION_PROJECTION in given_keys:
            if C.PARTITION_SCHEMA not in given_keys:
                raise NoParameterError(C.PARTITION_PROJECTION, req_param=C.PARTITION_SCHEMA)

            for column in metadata[C.PARTITION_PROJECTION]:
                if column not in metadata[C.PARTITION_SCHEMA]:
                    raise NoParameterError(
                        f'{C.PARTITION_SCHEMA} dictionary as it is in {C.PARTITION_PROJECTION}',
                        req_param=column
                    )

    @staticmethod
    def get_column_schema(
        column_schema: SchemaType
    ) -> str:
        """
        Function to return the string representation of the columns and their data types for Athena CREATE TABLE.

        :param column_schema: the dictionary of the columns with their data types
        """
        return ",\n\t".join(f'`{col}` {column_schema[col].ATHENA}' for col in column_schema)

    @staticmethod
    def get_storage_info(
        metadata: dict
    ) -> str:
        """
        Function to return the storage format part of the CREATE TABLE query.
        This includes the ROW FORMAT SERDE setting.

        :param metadata: the Schema metadata containing the S3 bucket and prefix keys
        """
        f_format = metadata.get(C.FILE_FORMAT).lower()

        hive_format = {
            C.PARQUET_: 'ql.io.parquet.serde.ParquetHiveSerDe',
            C.CSV_: 'serde2.OpenCSVSerde'
        }

        return f'org.apache.hadoop.hive.{hive_format[f_format]}'

    @staticmethod
    def get_s3_location(
        metadata: dict
    ) -> str:
        """
        Function to return the S3 URL of the location of the Athena table.

        :param metadata: the Schema metadata containing the S3 bucket and prefix keys
        :return: the S3 URL of the form s3://{bucket}/{prefix}
        """
        bucket = metadata.get(C.S3_BUCKET)
        prefix = metadata.get(C.S3_PREFIX)
        location = S3Url(bucket=bucket, prefix=prefix)

        return location.url

    @classmethod
    def get_partition_info(
        cls,
        metadata: dict
    ) -> str:
        """
        Function to return the PARTITIONED BY part of the CREATE TABLE query.

        :param metadata: the Schema metadata containing the S3 bucket and prefix keys
        :return: the formatted string containing the partition column schema, or '' if no partition columns are present
        """
        if metadata.get(C.PARTITION_SCHEMA):
            return f'PARTITIONED BY (\n\t{cls.get_column_schema(column_schema=metadata[C.PARTITION_SCHEMA])}\n)'
        else:
            return ''

    @classmethod
    def get_tbl_properties(
        cls,
        metadata: dict
    ) -> str:
        """
        Function to return the TBLPROPERTIES part of the CREATE TABLE query.
        This includes information about the file format, as well as partition projection if specified.

        :param metadata: the Schema metadata containing the S3 bucket and prefix keys
        """
        tbl_props = {}
        f_format = metadata.get(C.FILE_FORMAT).lower()

        if f_format == C.PARQUET_:
            tbl_props.update({
                'classification': C.PARQUET_,
                'compressionType': C.SNAPPY_
            })
        elif f_format == C.CSV_ and metadata.get(C.SKIP_HEADER):
            tbl_props.update({
                'skip.header.line.count': '1'
            })

        if C.PARTITION_PROJECTION in metadata:
            tbl_props.update({'projection.enabled': 'TRUE'})
            tbl_props.update(cls.get_partition_proj_properties(projection_dict=metadata[C.PARTITION_PROJECTION]))

        return ",\n\t".join(f"'{k}' = '{tbl_props[k]}'" for k in tbl_props)

    @classmethod
    def get_partition_proj_properties(
        cls,
        projection_dict: dict
    ) -> Dict[str, str]:
        """
        Function to return a dictionary of the TBLPROPERTIES relating to the partition projection.

        :param projection_dict: the partition projection dictionary as specified in the Schema
            this has the following format:
                {
                    'COLUMN_NAME': {
                        'type': 'ENUM'|'INTEGER'|'DATE'|'INJECTION',
                        **kwargs
                    }
                }
            see the documentation for a full explanation of the allowed values:
            https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        """
        tbl_props = {}

        for column in projection_dict:
            metadata = projection_dict[column]

            type_ = metadata.get('type').lower()
            supported_proj_types = ['enum', 'integer', 'date', 'injected']

            if type_ not in supported_proj_types:
                raise UnexpectedParameterError(param=type_, possible_values=supported_proj_types)

            function_name = f'get_{type_}_projection'
            tbl_props.update(cls.__getattribute__(cls, function_name)(column, metadata))

        return tbl_props

    @staticmethod
    def get_enum_projection(
        column: str,
        metadata: dict
    ) -> Dict[str, str]:
        """
        Function to get the TBLPROPERTIES for an ENUM projection.

        :param column: the column name for this projection
        :param metadata: the metadata containing information about the projection
        """
        values = metadata.get('values')

        if not values:
            raise NoParameterError(f'ENUM projection on column {column}', req_param='values')
        if not isinstance(values, list):
            raise AssertionError(f"The values attribute for ENUM projection must be a LIST; current: {type(values)}")

        return {
            f'projection.{column}.type': 'enum',
            f'projection.{column}.values': ','.join(values)
        }

    @staticmethod
    def get_integer_projection(
        column: str,
        metadata: dict
    ) -> Dict[str, str]:
        """
        Function to get the TBLPROPERTIES for an INTEGER projection.

        :param column: the column name for this projection
        :param metadata: the metadata containing information about the projection
        """
        kwargs = {}

        range_ = metadata.get('range')

        if not range_:
            raise NoParameterError(f'INTEGER projection on column {column}', req_param='range')

        for key in ['interval', 'digits']:
            if key in metadata:
                kwargs[f'projection.{column}.{key}'] = metadata.get(key)

        return {
            f'projection.{column}.type': 'integer',
            f'projection.{column}.range': range_,
            **kwargs
        }

    @staticmethod
    def get_date_projection(
        column: str,
        metadata: dict
    ) -> Dict[str, str]:
        """
        Function to get the TBLPROPERTIES for a DATE projection.

        :param column: the column name for this projection
        :param metadata: the metadata containing information about the projection
        """
        kwargs = {}

        range_ = metadata.get('range')
        format_ = metadata.get('format')

        if not range_:
            raise NoParameterError(f'DATE projection on column {column}', req_param='range')
        if not format_:
            raise NoParameterError(f'DATE projection on column {column}', req_param='format')

        for key in ['interval', 'interval.unit']:
            if key in metadata:
                kwargs[f'projection.{column}.{key}'] = metadata.get(key)

        return {
            f'projection.{column}.type': 'date',
            f'projection.{column}.range': range_,
            f'projection.{column}.format': format_,
            **kwargs
        }

    @staticmethod
    def get_injected_projection(
        column: str,
        *_
    ) -> Dict[str, str]:
        """
        Function to get the TBLPROPERTIES for an INJECTED projection.

        :param column: the column name for this projection
        """
        return {
            f'projection.{column}.type': 'injected'
        }
