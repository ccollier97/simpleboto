# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import os

from simpleboto.exceptions import (
    InvalidTypeError
)
from simpleboto.utils import Utils
from tests.base_test import BaseTest


class TestUtils(BaseTest):
    def setUp(self) -> None:
        super().setUp()

        self.test_data_dir = os.path.join(self.test_data_dir, 'test_utils')

    def test_get_file(self) -> None:
        self.assertEqual(
            Utils.get_file(location=os.path.join(self.test_data_dir, 'test_get_file.txt')),
            'THIS IS A\nTEST FILE'
        )

    def test_check_type_valid(self) -> None:
        valid = True
        try:
            Utils.check_type(
                key='TEST_VARIABLE',
                value=6,
                expected_type=int
            )
        except InvalidTypeError:
            valid = False

        self.assertTrue(valid)

    def test_check_type_invalid(self) -> None:
        with self.assertRaisesRegex(InvalidTypeError, r"Variable TEST_VARIABLE should have type <class 'int'>"):
            Utils.check_type(
                key='TEST_VARIABLE',
                value='6',
                expected_type=int
            )
