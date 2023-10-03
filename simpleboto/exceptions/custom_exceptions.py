# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import List, Optional, Any


class S3DelimiterError(Exception):
    """
    Exception class for S3 URLs with double slashes //.
    """
    def __init__(
        self,
        url: str
    ) -> None:
        super().__init__(f"The S3 URL {url} contains //")


class NoParameterError(Exception):
    """
    Exception class for when there are missing arguments in callables.
    """
    def __init__(
        self,
        callable_: str,
        req_param: Optional[Any] = None,
        arguments: Optional[List[str]] = None
    ) -> None:
        optional = f"; current call: {callable_}({', '.join(arguments)})" if arguments else ''
        super().__init__(
            f"Required parameter: {req_param} for {callable_}{optional}"
        )


class UnexpectedParameterError(Exception):
    """
    Exception class for specifying an unexpected key.
    """
    def __init__(
        self,
        param: Any,
        possible_values: Any
    ) -> None:
        super().__init__(f"The parameter(s) {param} is/are unexpected; must be one of {possible_values}")


class InvalidSchemaTypeError(Exception):
    """
    Exception class for specifying a data type not valid for a Schema.
    """
    def __init__(
        self,
        column: str,
        dtype: str
    ) -> None:
        super().__init__(f"The data type {dtype} is not valid for Schema column {column}")


class AttributeConditionError(Exception):
    """
    Exception class for specifying an invalid attribute value.
    """
    def __init__(
        self,
        attribute: str,
        class_name: Any,
        condition: str
    ) -> None:
        super().__init__(f"The {attribute} attribute of the {class_name} class does not satisfy: {condition}")
