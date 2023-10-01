# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Dict, Union, Optional

from simpleboto.athena.constants import C
from simpleboto.athena.utils.data_types import (
    DTypes,
    DecimalDType,
    VarCharDType
)
from simpleboto.exceptions import (
    InvalidSchemaType,
    AttributeConditionError
)

SchemaType = Dict[str, Union[*DTypes]]


class Schema:
    """
    Schema class containing all information about the relevant dataframe.
    """
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
            S3_BUCKET               The S3 bucket name where the data is stored in Athena
            S3_PREFIX               The S3 prefix where the data is stored in Athena
            FILE_FORMAT             The file format of the stored files
            FILE_COMPRESSION        The file compression of the stored files
            PARTITION_SCHEMA        A SchemaType object containing the partition columns (if required)
            PARTITION_PROJECTION    A dictionary containing the projection properties for each partition column
        """
        self.validate_schema(schema)
        self.raw = schema

        metadata = metadata if metadata else {}
        self.validate_metadata(metadata)
        self.metadata = metadata

    @classmethod
    def validate_schema(
        cls,
        schema: SchemaType,
        is_athena: Optional[bool] = False
    ) -> None:
        """
        Function to validate the input Schema, for example checking the correct data types are specified.

        :param schema: the Schema to validate
        :param is_athena: whether the Schema is for Athena use or not, as there are extra checks
            https://docs.aws.amazon.com/athena/latest/ug/create-table.html
        """
        for key in schema:
            c_dtype = schema[key]

            if not any(isinstance(c_dtype, dtype) for dtype in DTypes):
                raise InvalidSchemaType(column=key, dtype=c_dtype)

            if is_athena:
                cls.validate_dtype(c_dtype)

    @classmethod
    def validate_dtype(
        cls,
        current_dtype: Union[*DTypes]
    ) -> None:
        """
        Function to validate the specific data type.

        :param current_dtype: the current data type to validate
        """
        if isinstance(current_dtype, DecimalDType):
            if not 0 < current_dtype.precision <= 38:
                raise AttributeConditionError(
                    attribute='precision',
                    class_=DecimalDType.__name__,
                    condition=f'0 < attr ({current_dtype.precision}) <= 38'
                )
            if not 0 <= current_dtype.scale <= 38:
                raise AttributeConditionError(
                    attribute='scale',
                    class_=DecimalDType.__name__,
                    condition=f'0 <= attr ({current_dtype.scale}) <= 38'
                )
        elif isinstance(current_dtype, VarCharDType):
            if not 0 < current_dtype.length <= 65535:
                raise AttributeConditionError(
                    attribute='length',
                    class_=VarCharDType.__name__,
                    condition=f'0 < attr ({current_dtype.length}) <= 65535'
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

        if any(k not in valid_keys for k in keys):
            raise Exception()  # an unexpected key so raise an error

        if C.PARTITION_SCHEMA in keys:
            cls.validate_schema(metadata[C.PARTITION_SCHEMA])

        if C.PARTITION_PROJECTION in keys:
            assert isinstance(metadata[C.PARTITION_PROJECTION], dict), ""
            for column in metadata[C.PARTITION_PROJECTION]:
                assert isinstance(metadata[C.PARTITION_PROJECTION][column], dict), ""
