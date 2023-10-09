# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from simpleboto.athena import (
    VarCharDType,
    DecimalDType
)
from tests.base_test import BaseTest


class TestDataTypes(BaseTest):
    def test_varchar_repr(self) -> None:
        self.assertEqual(
            VarCharDType(100).__repr__(),
            'VarCharDType(100)'
        )

    def test_decimal_repr(self) -> None:
        self.assertEqual(
            DecimalDType(18, 8).__repr__(),
            'DecimalDType(18, 8)'
        )
