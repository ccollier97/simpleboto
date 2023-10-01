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
        req_param: Optional[str] = None,
        arguments: Optional[List[str]] = None
    ) -> None:
        arguments = arguments if arguments else []
        super().__init__(
            f"Required parameter: {req_param} for {callable_}; current call: {callable_}({', '.join(arguments)})"
        )


class InvalidSchemaType(Exception):
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
        class_: Any,
        condition: str
    ) -> None:
        super().__init__(f"The {attribute} attribute of the {class_} class does not satisfy: {condition}")
