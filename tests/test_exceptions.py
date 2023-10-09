# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from simpleboto.exceptions import (
    S3DelimiterError,
    InvalidTypeError,
    AttributeConditionError,
    UnexpectedParameterError,
    InvalidSchemaTypeError,
    NoParameterError
)
from tests.base_test import BaseTest


class TestExceptions(BaseTest):
    def test_s3_delimiter_error(self) -> None:
        with self.assertRaisesRegex(S3DelimiterError, 'The S3 URL TEST_URL contains //'):
            raise S3DelimiterError(url='TEST_URL')

    def test_invalid_type_error(self) -> None:
        with self.assertRaisesRegex(InvalidTypeError, 'Variable STRING should have type <class \'int\'>'):
            raise InvalidTypeError(variable='STRING', expected_type=int)

    def test_invalid_type_error_with_context(self) -> None:
        with self.assertRaisesRegex(InvalidTypeError, r"Variable STRING \(hi\) should have type <class 'int'>"):
            raise InvalidTypeError(variable='STRING', value='hi', expected_type=int)

    def test_attribute_condition_error_class_string(self) -> None:
        with self.assertRaisesRegex(
            AttributeConditionError,
            'The attribute ATTR of the class CLASS does not satisfy 0 < 1'
        ):
            raise AttributeConditionError(attribute='ATTR', class_='CLASS', condition='0 < 1')

    def test_attribute_condition_error_class(self) -> None:
        class FakeClass:
            def __init__(self) -> None:
                self.ATTR = 2

        with self.assertRaisesRegex(
            AttributeConditionError,
            'The attribute ATTR of the class FakeClass does not satisfy 0 < 1'
        ):
            raise AttributeConditionError(attribute='ATTR', class_=FakeClass, condition='0 < 1')

    def test_unexpected_parameter_error_single(self) -> None:
        with self.assertRaisesRegex(UnexpectedParameterError, 'The parameter TEST_PARAM is unexpected'):
            raise UnexpectedParameterError(param='TEST_PARAM')

    def test_unexpected_parameter_error_iterable(self) -> None:
        with self.assertRaisesRegex(
            UnexpectedParameterError,
            r"The parameters \('TEST_PARAM_1', 'TEST_PARAM_2'\) are unexpected"
        ):
            raise UnexpectedParameterError(param=('TEST_PARAM_1', 'TEST_PARAM_2'))

    def test_unexpected_parameter_error_with_possible_values(self) -> None:
        with self.assertRaisesRegex(
            UnexpectedParameterError,
            r"The parameter TEST_PARAM is unexpected; must be one of \['VAL1', 'VAL2'\]"
        ):
            raise UnexpectedParameterError(param='TEST_PARAM', possible_values=['VAL1', 'VAL2'])

    def test_unexpected_parameter_error_with_context(self) -> None:
        def test_func() -> None:
            return

        with self.assertRaisesRegex(
            UnexpectedParameterError,
            'The parameter TEST_PARAM is unexpected for test_func'
        ):
            raise UnexpectedParameterError(param='TEST_PARAM', context=test_func.__name__)

    def test_invalid_schema_type_error(self) -> None:
        with self.assertRaisesRegex(
            InvalidSchemaTypeError,
            "The data type <class 'int'> is not valid for column STR_COL"
        ):
            raise InvalidSchemaTypeError(dtype=int, column='STR_COL')

    def test_no_parameter_error(self) -> None:
        with self.assertRaisesRegex(NoParameterError, 'Required parameter TEST_PARAM'):
            raise NoParameterError(param='TEST_PARAM')

    def test_no_parameter_error_with_context(self) -> None:
        with self.assertRaisesRegex(NoParameterError, r'Required parameter TEST_PARAM for FUNCTION'):
            raise NoParameterError(param='TEST_PARAM', context='FUNCTION')

    def test_no_parameter_error_with_context_and_arguments(self) -> None:
        with self.assertRaisesRegex(NoParameterError, r'Required parameter TEST_PARAM for FUNCTION\(arg1, arg2\)'):
            raise NoParameterError(param='TEST_PARAM', context='FUNCTION', arguments=['arg1', 'arg2'])
