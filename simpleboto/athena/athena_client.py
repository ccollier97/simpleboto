# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import os
from typing import Optional, Dict, Any

import boto3

from simpleboto.athena.constants import C
from simpleboto.athena.utils.schema import Schema, SchemaType
from simpleboto.boto3_base import Boto3Base
from simpleboto.exceptions import (
    UnexpectedParameterError,
    NoParameterError
)
from simpleboto.s3.s3_url import S3Url
from simpleboto.utils import Utils


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

    @staticmethod
    def get_key(
        key: Any,
        dict_: dict
    ) -> Any:
        """
        Function to retrieve the key value from the dictionary dict_.

        :param key: the key of the dictionary to extract the value for
        :param dict_: the dictionary to search for the value
        """
        value = dict_.get(key)
        return value.lower() if isinstance(value, str) else value

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

        sql_template = Utils.get_file(location=os.path.join(cls.SQL_DIR, 'create_table.sql'))

        kwargs = {
            'database_name': cls.get_key(C.DATABASE_NAME, metadata) + '.' if C.DATABASE_NAME in metadata else '',
            'table_name': cls.get_key(C.TABLE_NAME, metadata),
            'column_schema': cls.get_column_schema(schema.raw),
            'row_format_serde': cls.get_serde(metadata),
            'location': cls.get_s3_location(metadata),
            'partitioned_by': cls.get_partition_info(metadata),
            'tbl_properties': cls.format_dict(cls.get_tbl_properties(metadata), kv_delimiter=' = ')
        }

        return sql_template.format(**kwargs)

    @classmethod
    def validate_metadata(
        cls,
        metadata: dict
    ) -> None:
        """
        Function to validate the metadata from the Schema for Athena.
        This checks it has the required columns and so on.

        :param metadata: the dictionary of metadata values
        """
        context = 'Schema metadata'

        given_keys = metadata.keys()
        missing_keys = [k for k in Schema.REQUIRED_ATHENA_FIELDS if k not in given_keys]

        if missing_keys:
            raise NoParameterError(param=missing_keys, context=context)

        if cls.get_key(C.FILE_FORMAT, metadata) not in Schema.REQUIRED_ATHENA_FORMAT:
            raise UnexpectedParameterError(
                param=C.FILE_FORMAT,
                context=context,
                possible_values=Schema.REQUIRED_ATHENA_FORMAT
            )

        if C.FILE_COMPRESSION in metadata:
            if cls.get_key(C.FILE_COMPRESSION, metadata) not in Schema.REQUIRED_ATHENA_COMPRESSION:
                raise UnexpectedParameterError(
                    param=C.FILE_COMPRESSION,
                    context=context,
                    possible_values=Schema.REQUIRED_ATHENA_COMPRESSION
                )

        cls.validate_metadata_partition(metadata=metadata, context=context)

    @staticmethod
    def validate_metadata_partition(
        metadata: dict,
        context: str
    ) -> None:
        """
        Function to validate the partition schema and partition projection parts of the Schema metadata.

        :param metadata: the dictionary of metadata values
        :param context: the base context for exception logging
        """
        given_keys = metadata.keys()

        if C.PARTITION_PROJECTION in given_keys:
            if C.PARTITION_SCHEMA in given_keys:
                partition_proj = metadata[C.PARTITION_PROJECTION]
                partition_schema = metadata[C.PARTITION_SCHEMA]

                if len(partition_proj) != len(partition_schema):
                    raise AssertionError(
                        f'{C.PARTITION_PROJECTION} and {C.PARTITION_SCHEMA} do not contain the same number of columns'
                    )

                miss_proj_cols = set(partition_schema.keys()).difference(partition_proj.keys())
                if miss_proj_cols:
                    raise NoParameterError(
                        param=miss_proj_cols,
                        context=f'{context} {C.PARTITION_PROJECTION} as it is present in {C.PARTITION_SCHEMA}'
                    )
            else:
                raise NoParameterError(
                    param=C.PARTITION_SCHEMA,
                    context=f'{context} if {C.PARTITION_PROJECTION} is used'
                )

    @classmethod
    def get_column_schema(
        cls,
        column_schema: SchemaType
    ) -> str:
        """
        Function to return the string representation of the columns and their data types for Athena CREATE TABLE.

        :param column_schema: the dictionary of the columns with their data types
        """
        return cls.format_dict({f'`{col}`': dtype.ATHENA for col, dtype in column_schema.items()})

    @staticmethod
    def format_dict(
        dict_: dict,
        kv_delimiter: Optional[str] = ' ',
        line_delimiter: Optional[str] = ',\n\t'
    ) -> str:
        """
        Function to take an input dictionary and transform it into a string

        :param dict_: the dictionary to turn into a string
        :param kv_delimiter: the key-value delimiter, e.g. {key: value} becomes f'{key}{kv_delimiter}{value}'
        :param line_delimiter: the delimiter between each key of the dictionary, e.g. f'{kv1}{line_delimiter}{kv2}'
        """
        return line_delimiter.join([f'{k}{kv_delimiter}{v}' for k, v in dict_.items()])

    @classmethod
    def get_serde(
        cls,
        metadata: dict
    ) -> str:
        """
        Function to return the SERDE for the CREATE TABLE query.

        :param metadata: the Schema metadata containing the S3 bucket and prefix keys
        """
        serde = {
            C.PARQUET_: 'ql.io.parquet.serde.ParquetHiveSerDe',
            C.CSV_: 'serde2.OpenCSVSerde'
        }
        f_format = cls.get_key(C.FILE_FORMAT, metadata)

        return f'org.apache.hadoop.hive.{serde[f_format]}'

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

        return S3Url(bucket=bucket, prefix=prefix).url

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
        partition_str = ''
        partition_schema = metadata.get(C.PARTITION_SCHEMA)

        if partition_schema:
            column_schema = cls.get_column_schema(column_schema=partition_schema)
            partition_str = f'\nPARTITIONED BY (\n\t{column_schema}\n)'

        return partition_str

    @classmethod
    def get_tbl_properties(
        cls,
        metadata: dict
    ) -> dict:
        """
        Function to return the TBLPROPERTIES part of the CREATE TABLE query.
        This includes information about the file format, as well as partition projection if specified.

        :param metadata: the Schema metadata containing the S3 bucket and prefix keys
        """
        tbl_props = {}

        f_format = cls.get_key(C.FILE_FORMAT, metadata)
        f_compression = cls.get_key(C.FILE_COMPRESSION, metadata)

        tbl_props.update({'classification': f_format})

        if f_compression:
            tbl_props.update({'compressionType': f_compression})

        if f_format == C.CSV_ and metadata.get(C.SKIP_HEADER):
            tbl_props.update({'skip.header.line.count': '1'})

        if C.PARTITION_PROJECTION in metadata:
            tbl_props.update({'projection.enabled': 'TRUE'})
            tbl_props.update(cls.get_partition_proj_properties(projection_dict=metadata[C.PARTITION_PROJECTION]))

        return {f"'{k}'": f"'{prop}'" for k, prop in tbl_props.items()}

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
                        'type': 'ENUM'|'INTEGER'|'DATE'|'INJECTED',
                        **kwargs
                    }
                }
            see the documentation for a full explanation of the allowed values:
            https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        """
        tbl_props = {}
        supported_proj_types = ['enum', 'integer', 'date', 'injected']

        for column in projection_dict:
            metadata = projection_dict[column]

            type_ = cls.get_key('type', metadata)
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
            raise NoParameterError(param='values', context=f'ENUM projection on column {column}')

        Utils.check_type(key='values for ENUM projection', value=values, expected_type=list)

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
            raise NoParameterError(param='range', context=f'INTEGER projection on column {column}')

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
            raise NoParameterError(param='range', context=f'DATE projection on column {column}')
        if not format_:
            raise NoParameterError(param='format', context=f'DATE projection on column {column}')

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
