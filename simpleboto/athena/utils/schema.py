# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Dict, Optional, TypeVar

from simpleboto.athena.constants import C
from simpleboto.athena.utils.data_types import (
    DTypes,
    DecimalDType,
    VarCharDType
)
from simpleboto.exceptions import (
    InvalidSchemaTypeError,
    AttributeConditionError,
    UnexpectedParameterError
)
from simpleboto.utils import Utils

DataTypes = TypeVar('DataTypes')
SchemaType = Dict[str, DataTypes]


class Schema:
    """
    Schema class containing all information about the relevant dataframe.
    """
    REQUIRED_ATHENA_FIELDS = [
        C.TABLE_NAME,
        C.S3_BUCKET,
        C.S3_PREFIX,
        C.FILE_FORMAT
    ]
    REQUIRED_ATHENA_FORMAT = [
        C.PARQUET_,
        C.CSV_
    ]
    REQUIRED_ATHENA_COMPRESSION = [
        C.SNAPPY_,
        C.GZIP_
    ]

    def __init__(
        self,
        schema: SchemaType,
        metadata: Optional[dict] = None
    ) -> None:
        """
        :param schema: the Schema dictionary for the given data
            an example Schema is as such:
            {
                'COLUMN 1': StringDType(),
                'COLUMN 2': DecimalDType(10, 6),
                'COLUMN 3': BooleanDType()
            }
        :param metadata: a dictionary containing optional metadata about the given Schema; compatible keys include:
            DATABASE_NAME        [str]        The Athena database name where the Schema exists
            TABLE_NAME           [str]        The Athena table name where the Schema exists
            S3_BUCKET            [str]        The S3 bucket name where the data is stored in Athena
            S3_PREFIX            [str]        The S3 prefix where the data is stored in Athena
            FILE_FORMAT          [str]        The file format of the stored files
            FILE_COMPRESSION     [str]        The file compression of the stored files
            SKIP_HEADER          [bool]       Whether to skip the header row in CSV files or not in Athena
            PARTITION_SCHEMA     [SchemaType] A SchemaType object containing the partition columns (if required)
            PARTITION_PROJECTION [dict]       A dict containing the projection properties for each partition column
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
        self.validate_schema(schema)
        self.raw = schema

        metadata = metadata if metadata else {}
        self.validate_metadata(metadata)
        self.metadata = metadata

    @classmethod
    def validate_schema(
        cls,
        schema: SchemaType
    ) -> None:
        """
        Function to validate the input Schema, for example checking the correct data types are specified.

        :param schema: the Schema to validate
        """
        for key in schema:
            c_dtype = schema[key]

            if not any(isinstance(c_dtype, dtype) for dtype in DTypes):
                raise InvalidSchemaTypeError(column=key, dtype=c_dtype)

            cls.validate_dtype(c_dtype)

    @classmethod
    def validate_dtype(
        cls,
        current_dtype: DataTypes
    ) -> None:
        """
        Function to validate the specific data type.

        :param current_dtype: the current data type to validate
            https://docs.aws.amazon.com/athena/latest/ug/create-table.html
        """
        if isinstance(current_dtype, DecimalDType):
            if not 0 < current_dtype.precision <= 38:
                raise AttributeConditionError(
                    attribute='precision',
                    class_=DecimalDType,
                    condition=f'0 < x ({current_dtype.precision}) <= 38'
                )
            if not 0 <= current_dtype.scale <= 38:
                raise AttributeConditionError(
                    attribute='scale',
                    class_=DecimalDType,
                    condition=f'0 <= x ({current_dtype.scale}) <= 38'
                )
        elif isinstance(current_dtype, VarCharDType):
            if not 0 < current_dtype.length <= 65535:
                raise AttributeConditionError(
                    attribute='length',
                    class_=VarCharDType,
                    condition=f'0 < x ({current_dtype.length}) <= 65535'
                )

    @classmethod
    def validate_metadata(
        cls,
        metadata: dict
    ) -> None:
        """
        Function to validate the metadata given with the Schema, checking for correct types.

        :param metadata: the metadata dictionary to validate
        """
        keys = metadata.keys()
        valid_keys = [k for k in vars(C) if '__' not in k and not k.endswith('_')]
        missing_keys = [k for k in keys if k not in valid_keys]

        if missing_keys:
            raise UnexpectedParameterError(param=missing_keys, possible_values=valid_keys)

        if C.SKIP_HEADER in keys:
            Utils.check_type(key=C.SKIP_HEADER, value=metadata[C.SKIP_HEADER], expected_type=bool)

        if C.PARTITION_SCHEMA in keys:
            cls.validate_schema(metadata[C.PARTITION_SCHEMA])

        if C.PARTITION_PROJECTION in keys:
            Utils.check_type(key=C.PARTITION_PROJECTION, value=metadata[C.PARTITION_PROJECTION], expected_type=dict)

            for column in metadata[C.PARTITION_PROJECTION]:
                Utils.check_type(
                    key=f'{C.PARTITION_PROJECTION}[{column}]',
                    value=metadata[C.PARTITION_PROJECTION][column],
                    expected_type=dict
                )
