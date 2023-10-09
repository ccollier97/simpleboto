# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import inspect
from collections.abc import Iterable as Iter
from typing import (
    Any,
    Type,
    Iterable,
    Optional
)


class S3DelimiterError(Exception):
    """
    Exception class for S3 URLs with double slashes //.
    """
    def __init__(
        self,
        url: str
    ) -> None:
        """
        :param url: the URL which is invalid
        """
        self.url = url

        self.err_msg = f"The S3 URL {url} contains //"

        super().__init__(self.err_msg)


class InvalidTypeError(Exception):
    """
    Exception class for when a variable is not of the required type.
    """
    def __init__(
        self,
        variable: Any,
        expected_type: Type,
        value: Optional[Any] = None
    ) -> None:
        """
        :param variable: the name of the variable which has invalid type
        :param expected_type: the expected type for the variable
        :param value: the actual value of the variable, for logging in the exception
        """
        self.variable = variable
        self.value = value
        self.expected_type = expected_type

        value_str = f' ({self.value})' if value else ''

        self.err_msg = f"Variable {self.variable}{value_str} should have type {self.expected_type}"

        super().__init__(self.err_msg)


class AttributeConditionError(Exception):
    """
    Exception class for specifying an invalid attribute value.
    """
    def __init__(
        self,
        attribute: str,
        class_: Any,
        condition: str
    ) -> None:
        """
        :param attribute: the name of the attribute
        :param class_: the class the attribute belongs to
        :param condition: the condition which is causing the error to be raised
        """
        self.attribute = attribute
        self.class_ = class_.__name__ if inspect.isclass(class_) else class_
        self.condition = condition

        self.err_msg = f"The attribute {self.attribute} of the class {self.class_} does not satisfy {self.condition}"

        super().__init__(self.err_msg)


class UnexpectedParameterError(Exception):
    """
    Exception class for specifying an unexpected key.
    """
    def __init__(
        self,
        param: Any,
        possible_values: Optional[Any] = None,
        context: Optional[str] = None
    ) -> None:
        """
        :param param: the parameter name which is unexpected
        :param possible_values: the possible values for this parameter
        :param context: the context for the error, e.g. a method name
        """
        self.param = param
        self.possible_values = possible_values
        self.context = context

        base_msg = 'The parameter{} {param} {} unexpected{}{}'
        param_sub = ['', 'is', '', '']

        if isinstance(param, Iter) and not isinstance(param, str):
            param_sub[0] = 's'
            param_sub[1] = 'are'
        if context:
            param_sub[2] = f' for {context}'
        if possible_values:
            param_sub[3] = f'; must be one of {possible_values}'

        self.err_msg = base_msg.format(*param_sub, param=self.param)

        super().__init__(self.err_msg)


class NoParameterError(Exception):
    """
    Exception class for when there are missing arguments in callables.
    """
    def __init__(
        self,
        param: Any,
        context: Optional[str] = None,
        arguments: Optional[Iterable] = None
    ) -> None:
        """
        :param param: the parameter which is missing
        :param context: the callable from where the parameter is missing
        :param arguments: the given arguments to the callable
        """
        self.param = param
        self.context = context
        self.arguments = arguments

        base_msg = 'Required parameter {param}{}'
        args, param_sub = '', ''

        if context:
            param_sub = f' for {context}'
            if arguments:
                args = ', '.join(arguments)
                param_sub = f'{param_sub}({args})'

        self.err_msg = base_msg.format(param_sub, param=param)

        super().__init__(self.err_msg)


class InvalidSchemaTypeError(Exception):
    """
    Exception class for specifying a data type not valid for a Schema.
    """
    def __init__(
        self,
        dtype: Any,
        column: str
    ) -> None:
        """
        :param dtype: the data type of the current column
        :param column: the column containing the incorrect data type
        """
        self.dtype = dtype
        self.column = column

        self.err_msg = f"The data type {dtype} is not valid for column {column}"

        super().__init__(self.err_msg)
