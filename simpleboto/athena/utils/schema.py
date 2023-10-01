# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Dict, Union, Optional

from simpleboto.athena.constants import C
from simpleboto.athena.utils.data_types import DTypes
from simpleboto.exceptions import InvalidSchemaType

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
            if not any(isinstance(schema[key], dtype) for dtype in DTypes):
                raise InvalidSchemaType(column=key, dtype=schema[key])

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

        if C.FILE_FORMAT in keys:
            pass
        if C.FILE_COMPRESSION in keys:
            pass
        if C.PARTITION_SCHEMA in keys:
            cls.validate_schema(metadata[C.PARTITION_SCHEMA])
        if C.PARTITION_PROJECTION in keys:
            pass
