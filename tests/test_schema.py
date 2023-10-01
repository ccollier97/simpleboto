# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from simpleboto.athena import Schema
from simpleboto.athena.utils import StringDType, TimestampDType
from simpleboto.exceptions import (
    InvalidSchemaType
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
