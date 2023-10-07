# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import os

from moto import mock_athena

from simpleboto.athena.athena_client import AthenaClient
from simpleboto.athena.constants import C
from simpleboto.athena.utils.data_types import (
    StringDType,
    IntegerDType,
    DecimalDType
)
from simpleboto.exceptions import (
    NoParameterError,
    InvalidTypeError,
    UnexpectedParameterError
)
from tests.base_test import BaseTest

TEST_COLUMN = 'TEST_COLUMN'


@mock_athena
class TestAthenaClient(BaseTest):
    def setUp(self) -> None:
        super().setUp()

        self.test_class = 'test_athena_client'
        self.test_data_dir = os.path.join(self.test_data_dir, self.test_class)

        self.ac = AthenaClient(region_name=self.env_vars['REGION'])

    def test_get_injected_projection(self) -> None:
        self.assertEqual(
            AthenaClient.get_injected_projection(column=TEST_COLUMN),
            {
                f'projection.{TEST_COLUMN}.type': 'injected'
            }
        )

    def test_get_date_projection(self) -> None:
        self.assertEqual(
            AthenaClient.get_date_projection(
                column=TEST_COLUMN,
                metadata={
                    'range': '2023-01-01,NOW',
                    'format': 'yyyy-MM-dd',
                    'interval': 1,
                    'interval.unit': 'DAY'
                }
            ),
            {
                f'projection.{TEST_COLUMN}.format': 'yyyy-MM-dd',
                f'projection.{TEST_COLUMN}.interval': 1,
                f'projection.{TEST_COLUMN}.interval.unit': 'DAY',
                f'projection.{TEST_COLUMN}.range': '2023-01-01,NOW',
                f'projection.{TEST_COLUMN}.type': 'date',
            }
        )

    def test_get_date_projection_no_range(self) -> None:
        with self.assertRaisesRegex(
            NoParameterError,
            'Required parameter range for DATE projection on column TEST_COLUMN'
        ):
            AthenaClient.get_date_projection(
                column=TEST_COLUMN,
                metadata={
                    'format': 'yyyy-MM-dd'
                }
            )

    def test_get_date_projection_no_format(self) -> None:
        with self.assertRaisesRegex(
            NoParameterError,
            'Required parameter format for DATE projection on column TEST_COLUMN'
        ):
            AthenaClient.get_date_projection(
                column=TEST_COLUMN,
                metadata={
                    'range': 'NOW-1YEAR,NOW'
                }
            )

    def test_get_integer_projection(self) -> None:
        self.assertEqual(
            AthenaClient.get_integer_projection(
                column=TEST_COLUMN,
                metadata={
                    'range': '0,20',
                    'interval': 1,
                    'digits': 2
                }
            ),
            {
                f'projection.{TEST_COLUMN}.digits': 2,
                f'projection.{TEST_COLUMN}.interval': 1,
                f'projection.{TEST_COLUMN}.range': '0,20',
                f'projection.{TEST_COLUMN}.type': 'integer'
            }
        )

    def test_get_integer_projection_no_range(self) -> None:
        with self.assertRaisesRegex(
            NoParameterError,
            'Required parameter range for INTEGER projection on column TEST_COLUMN'
        ):
            AthenaClient.get_integer_projection(
                column=TEST_COLUMN,
                metadata={
                    'interval': 1
                }
            )

    def test_get_enum_projection(self) -> None:
        self.assertEqual(
            AthenaClient.get_enum_projection(
                column=TEST_COLUMN,
                metadata={
                    'values': ['A', 'B']
                }
            ),
            {
                f'projection.{TEST_COLUMN}.type': 'enum',
                f'projection.{TEST_COLUMN}.values': 'A,B'
            }
        )

    def test_get_enum_projection_no_values(self) -> None:
        with self.assertRaisesRegex(
            NoParameterError,
            'Required parameter values for ENUM projection on column TEST_COLUMN'
        ):
            AthenaClient.get_enum_projection(
                column=TEST_COLUMN,
                metadata={}
            )

    def test_get_enum_projection_values_type_error(self) -> None:
        with self.assertRaisesRegex(
            InvalidTypeError,
            "Variable values for ENUM projection should have type <class 'list'>"
        ):
            AthenaClient.get_enum_projection(
                column=TEST_COLUMN,
                metadata={
                    'values': 'A,B,C'
                }
            )

    def test_get_partition_proj_properties(self) -> None:
        self.assertEqual(
            AthenaClient.get_partition_proj_properties(
                projection_dict={
                    'COLUMN1': {
                        'type': 'enum',
                        'values': ['A', 'B']
                    },
                    'COLUMN2': {
                        'type': 'integer',
                        'range': '0,23',
                        'interval': 1,
                        'digits': 2
                    },
                    'COLUMN3': {
                        'type': 'date',
                        'format': 'yyyy-MM-dd',
                        'range': 'NOW-2YEARS,NOW'
                    },
                    'COLUMN4': {
                        'type': 'injected'
                    }
                }
            ),
            {
                'projection.COLUMN1.type': 'enum',
                'projection.COLUMN1.values': 'A,B',
                'projection.COLUMN2.digits': 2,
                'projection.COLUMN2.interval': 1,
                'projection.COLUMN2.range': '0,23',
                'projection.COLUMN2.type': 'integer',
                'projection.COLUMN3.format': 'yyyy-MM-dd',
                'projection.COLUMN3.range': 'NOW-2YEARS,NOW',
                'projection.COLUMN3.type': 'date',
                'projection.COLUMN4.type': 'injected'
            }
        )

    def test_get_partition_proj_properties_unsupported_type(self) -> None:
        with self.assertRaisesRegex(
            UnexpectedParameterError,
            'The parameter fake_type is unexpected; must be one of'
        ):
            AthenaClient.get_partition_proj_properties(
                projection_dict={
                    TEST_COLUMN: {
                        'type': 'FAKE_TYPE'
                    }
                }
            )

    def test_get_tbl_properties_parquet(self) -> None:
        self.assertEqual(
            AthenaClient.get_tbl_properties(
                metadata={
                    C.FILE_FORMAT: C.PARQUET_,
                    C.FILE_COMPRESSION: C.SNAPPY_
                }
            ),
            {
                "'classification'": "'parquet'",
                "'compressionType'": "'snappy'"
            }
        )

    def test_get_tbl_properties_csv(self) -> None:
        self.assertEqual(
            AthenaClient.get_tbl_properties(
                metadata={
                    C.FILE_FORMAT: C.CSV_,
                    C.FILE_COMPRESSION: C.GZIP_
                }
            ),
            {
                "'classification'": "'csv'",
                "'compressionType'": "'gzip'"
            }
        )

    def test_get_tbl_properties_csv_skip_header(self) -> None:
        self.assertEqual(
            AthenaClient.get_tbl_properties(
                metadata={
                    C.FILE_FORMAT: C.CSV_,
                    C.FILE_COMPRESSION: C.GZIP_,
                    C.SKIP_HEADER: True
                }
            ),
            {
                "'classification'": "'csv'",
                "'compressionType'": "'gzip'",
                "'skip.header.line.count'": "'1'"
            }
        )

    def test_get_tbl_properties_partition_projection(self) -> None:
        self.assertEqual(
            AthenaClient.get_tbl_properties(
                metadata={
                    C.FILE_FORMAT: C.PARQUET_,
                    C.FILE_COMPRESSION: C.SNAPPY_,
                    C.PARTITION_PROJECTION: {
                        TEST_COLUMN: {
                            'type': 'enum',
                            'values': ['A', 'B']
                        }
                    }
                }
            ),
            {
                "'classification'": "'parquet'",
                "'compressionType'": "'snappy'",
                f"'projection.{TEST_COLUMN}.type'": "'enum'",
                f"'projection.{TEST_COLUMN}.values'": "'A,B'",
                "'projection.enabled'": "'TRUE'"
            }
        )

    def test_get_partition_info(self) -> None:
        self.assertEqual(
            AthenaClient.get_partition_info(
                metadata={
                    C.PARTITION_SCHEMA: {
                        'COLUMN1': StringDType(),
                        'COLUMN2': IntegerDType()
                    }
                }
            ),
            'PARTITIONED BY (\n\t`COLUMN1` string,\n\t`COLUMN2` integer\n)'
        )

    def test_get_partition_info_no_partitions(self) -> None:
        self.assertEqual(
            AthenaClient.get_partition_info(
                metadata={}
            ),
            ''
        )

    def test_get_s3_location(self) -> None:
        self.assertEqual(
            AthenaClient.get_s3_location(
                metadata={
                    C.S3_BUCKET: 'TEST-BUCKET',
                    C.S3_PREFIX: 'TEST/PREFIX'
                }
            ),
            's3://TEST-BUCKET/TEST/PREFIX/'
        )

    def test_get_serde_parquet(self) -> None:
        self.assertEqual(
            AthenaClient.get_serde(
                metadata={
                    C.FILE_FORMAT: C.PARQUET_
                }
            ),
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        )

    def test_get_serde_csv(self) -> None:
        self.assertEqual(
            AthenaClient.get_serde(
                metadata={
                    C.FILE_FORMAT: C.CSV_
                }
            ),
            'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        )

    def test_format_dict(self) -> None:
        self.assertEqual(
            AthenaClient.format_dict(
                dict_={
                    'KEY1': 'VALUE1',
                    'KEY2': 'VALUE2'
                },
            ),
            'KEY1 VALUE1,\n\tKEY2 VALUE2'
        )

    def test_format_dict_non_default_delimiter(self) -> None:
        self.assertEqual(
            AthenaClient.format_dict(
                dict_={
                    'KEY1': 'VALUE1',
                    'KEY2': 'VALUE2'
                },
                kv_delimiter='|',
                line_delimiter='\n'
            ),
            'KEY1|VALUE1\nKEY2|VALUE2'
        )

    def test_get_column_schema(self) -> None:
        self.assertEqual(
            AthenaClient.get_column_schema(
                column_schema={
                    'COLUMN1': StringDType(),
                    'COLUMN2': DecimalDType(10, 6),
                    'COLUMN3': IntegerDType()
                }
            ),
            '`COLUMN1` string,\n\t`COLUMN2` decimal(10, 6),\n\t`COLUMN3` integer'
        )

    def test_get_key(self) -> None:
        self.assertEqual(
            AthenaClient.get_key(key_='KEY', dict_={'KEY': ['A', 'B']}),
            ['A', 'B']
        )

    def test_get_key_string(self) -> None:
        self.assertEqual(
            AthenaClient.get_key(key_='KEY', dict_={'KEY': 'VALUE'}),
            'value'
        )
