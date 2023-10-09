# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Type, Any

from simpleboto.exceptions import InvalidTypeError


class Utils:
    @classmethod
    def get_file(
        cls,
        location: str
    ) -> str:
        """
        Function to load the contents of the file and return it as a string.

        :param location: the location of the file to open
        """
        with open(location, 'r') as f:
            return f.read()

    @classmethod
    def check_type(
        cls,
        key: str,
        value: Any,
        expected_type: Type
    ) -> None:
        """
        Function to check the type of value is of expected type, and raise an Exception if not.

        :param key: the key and identifier for the exception logging
        :param value: the value to check
        :param expected_type: the expected type for value
        """
        if not isinstance(value, expected_type):
            raise InvalidTypeError(variable=key, expected_type=expected_type)
