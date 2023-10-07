# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import List, Type

from simpleboto.exceptions import InvalidTypeError


class Utils:
    @classmethod
    def get_provided_parameters(
        cls,
        parameter_dict: dict
    ) -> List[str]:
        """
        Function to return a list of the input variables which were provided.

        :param parameter_dict: dictionary with key-values 'parameterName' and 'parameterValue'
        """
        non_null_params = {k: v for k, v in parameter_dict.items() if v}

        return list(non_null_params.keys())

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
        value: str,
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
