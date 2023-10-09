# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from simpleboto.athena import Schema
from simpleboto.athena.constants import C
from simpleboto.athena.utils import (
    StringDType,
    TimestampDType,
    DecimalDType,
    VarCharDType
)
from simpleboto.exceptions import (
    InvalidSchemaTypeError,
    AttributeConditionError,
    UnexpectedParameterError,
    InvalidTypeError
)
from tests.base_test import BaseTest


class TestSchema(BaseTest):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = Schema

    def test_validate_schema_valid(self) -> None:
        valid = True
        try:
            Schema.validate_schema(schema={
                'COL1': StringDType(),
                'COL2': TimestampDType()
            })
        except InvalidSchemaTypeError:
            valid = False

        self.assertTrue(valid)

    def test_validate_schema_invalid(self) -> None:
        with self.assertRaisesRegex(InvalidSchemaTypeError, "The data type string is not valid for column COL2"):
            Schema.validate_schema(schema={
                'COL1': StringDType(),
                'COL2': 'string'
            })

    def test_validate_schema_invalid_dtype_error(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            r"The attribute length of the class VarCharDType does not satisfy 0 < x \(0\) <= 65535"
        ):
            Schema.validate_schema(schema={
                'COL1': VarCharDType(0),
                'COL2': StringDType()
            })

    def test_validate_dtype_decimal_invalid_precision(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            r"The attribute precision of the class DecimalDType does not satisfy 0 < x \(77\) <= 38"
        ):
            Schema.validate_dtype(current_dtype=DecimalDType(77, 5))

    def test_validate_dtype_decimal_invalid_scale(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            r"The attribute scale of the class DecimalDType does not satisfy 0 <= x \(-1\) <= 38"
        ):
            Schema.validate_dtype(current_dtype=DecimalDType(10, -1))

    def test_validate_decimal_precision_scale(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "PRECISION must be greater than or equal to SCALE for DecimalDType"
        ):
            DecimalDType(10, 11)

    def test_validate_dtype_invalid_varchar_length(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            r"The attribute length of the class VarCharDType does not satisfy 0 < x \(99999\) <= 65535"
        ):
            Schema.validate_dtype(current_dtype=VarCharDType(99_999))

    def test_validate_metadata_missing_keys(self) -> None:
        with self.assertRaisesRegex(UnexpectedParameterError, r"The parameters \['FAKE_KEY'\] are unexpected"):
            Schema.validate_metadata(metadata={'FAKE_KEY': ''})

    def test_validate_metadata_skip_header_type(self) -> None:
        with self.assertRaisesRegex(InvalidTypeError, rf"Variable {C.SKIP_HEADER} should have type <class 'bool'>"):
            Schema.validate_metadata(metadata={C.SKIP_HEADER: 'NOT_A_BOOL'})

    def test_validate_metadata_partition_projection_type(self) -> None:
        with self.assertRaisesRegex(
            InvalidTypeError,
            rf"Variable {C.PARTITION_PROJECTION} should have type <class 'dict'>"
        ):
            Schema.validate_metadata(metadata={C.PARTITION_PROJECTION: 'NOT_A_DICT'})

    def test_validate_metadata_partition_projection_column_type(self) -> None:
        with self.assertRaisesRegex(
            InvalidTypeError,
            rf"Variable {C.PARTITION_PROJECTION}\[COLUMN1\] should have type <class 'dict'>"
        ):
            Schema.validate_metadata(metadata={
                C.PARTITION_PROJECTION: {
                    'COLUMN1': 'NOT_A_DICT'
                }
            })

    def test_validate_metadata_partition_schema_invalid(self) -> None:
        with self.assertRaisesRegex(InvalidSchemaTypeError, 'The data type String is not valid for column COLUMN1'):
            Schema.validate_metadata(metadata={
                C.PARTITION_SCHEMA: {
                    'COLUMN1': 'String'
                }
            })
