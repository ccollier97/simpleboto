# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import sys


class BaseDType:
    def __repr__(
        self
    ) -> str:
        return self.__class__.__name__


class StringDType(BaseDType):
    ATHENA = 'string'


class IntegerDType(BaseDType):
    ATHENA = 'integer'


class BigIntDType(BaseDType):
    ATHENA = 'bigint'


class DoubleDType(BaseDType):
    ATHENA = 'double'


class FloatDType(BaseDType):
    ATHENA = 'float'


class BooleanDType(BaseDType):
    ATHENA = 'boolean'


class DecimalDType(BaseDType):
    def __init__(
        self,
        precision: int,
        scale: int
    ) -> None:
        self.precision = precision
        self.scale = scale

        self.ATHENA = f'decimal({self.precision}, {self.scale})'

    def __repr__(
        self
    ) -> str:
        return f'{super().__repr__()}({self.precision}, {self.scale})'


class VarCharDType(BaseDType):
    def __init__(
        self,
        length: int
    ) -> None:
        self.length = length

        self.ATHENA = f'varchar({length})'

    def __repr__(
        self
    ) -> str:
        return f'{super().__repr__()}({self.length})'


class TimestampDType(BaseDType):
    ATHENA = 'timestamp'


class DateDType(BaseDType):
    ATHENA = 'date'


DTypes = [getattr(sys.modules[__name__], cls) for cls in dir() if cls.endswith('DType') and 'Base' not in cls]
