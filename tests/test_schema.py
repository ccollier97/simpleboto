# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import re

from simpleboto.athena import Schema
from simpleboto.athena.utils import (
    StringDType,
    TimestampDType,
    DecimalDType,
    VarCharDType
)
from simpleboto.exceptions import (
    InvalidSchemaType,
    AttributeConditionError
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
        except InvalidSchemaType:
            valid = False

        self.assertTrue(valid)

    def test_validate_schema_invalid(self) -> None:
        with self.assertRaisesRegex(InvalidSchemaType, "The data type string is not valid for Schema column COL2"):
            Schema.validate_schema(schema={
                'COL1': StringDType(),
                'COL2': 'string'
            })

    def test_validate_schema_invalid_dtype_error(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            re.compile(r"The length attribute of the VarCharDType class does not satisfy: 0 < attr \(0\) <= 65535")
        ):
            Schema.validate_schema(is_athena=True, schema={
                'COL1': VarCharDType(0),
                'COL2': StringDType()
            })

    def test_validate_dtype_decimal_invalid_precision(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            re.compile(r"The precision attribute of the DecimalDType class does not satisfy: 0 < attr \(77\) <= 38")
        ):
            Schema.validate_dtype(current_dtype=DecimalDType(77, 5))

    def test_validate_dtype_decimal_invalid_scale(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            re.compile(r"The scale attribute of the DecimalDType class does not satisfy: 0 <= attr \(-1\) <= 38")
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
            re.compile(r"The length attribute of the VarCharDType class does not satisfy: 0 < attr \(99999\) <= 65535")
        ):
            Schema.validate_dtype(current_dtype=VarCharDType(99_999))
