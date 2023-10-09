# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from simpleboto import S3Url
from simpleboto.exceptions import (
    S3DelimiterError,
    NoParameterError
)
from tests.base_test import BaseTest


class TestS3Url(BaseTest):
    @classmethod
    def setUpClass(cls) -> None:
        cls.bucket = 'BUCKET_NAME'
        cls.key = 'TEST/KEY'
        cls.prefix = 'TEST'
        cls.file = 'FILE'

    def test_invalid_delimiter_url(self) -> None:
        with self.assertRaisesRegex(
            S3DelimiterError,
            'The S3 URL s3://BUCKET_NAME//TEST/KEY contains //'
        ):
            S3Url(url=f's3://{self.bucket}//{self.key}')

    def test_key_no_bucket(self) -> None:
        with self.assertRaisesRegex(NoParameterError, 'Required parameter bucket for S3Url'):
            S3Url(key=self.key)

    def test_prefix_no_bucket(self) -> None:
        with self.assertRaisesRegex(NoParameterError, 'Required parameter bucket for S3Url'):
            S3Url(prefix=self.prefix)

    def test_no_parameters(self) -> None:
        with self.assertRaisesRegex(NoParameterError, 'Required parameter ANY for S3Url'):
            S3Url()

    def test_url_without_protocol(self) -> None:
        self.assertEqual(
            S3Url(url=f'{self.bucket}/{self.key}'),
            f's3://{self.bucket}/{self.key}'
        )

    def test_url_with_protocol(self) -> None:
        self.assertEqual(
            S3Url(url=f's3://{self.bucket}/{self.key}'),
            f's3://{self.bucket}/{self.key}'
        )

    def test_bucket_name_only(self) -> None:
        self.assertEqual(
            S3Url(bucket=self.bucket),
            f's3://{self.bucket}'
        )

    def test_bucket_name_with_key(self) -> None:
        self.assertEqual(
            S3Url(bucket=self.bucket, key=self.key),
            f's3://{self.bucket}/{self.key}'
        )

    def test_bucket_name_with_prefix(self) -> None:
        self.assertEqual(
            S3Url(bucket=self.bucket, prefix=self.prefix),
            f's3://{self.bucket}/{self.prefix}/'
        )

    def test_key_ignore_prefix(self) -> None:
        self.assertEqual(
            S3Url(bucket=self.bucket, key=self.key, prefix='WRONG'),
            f's3://{self.bucket}/{self.key}'
        )

    def test_url_attributes_full(self) -> None:
        s3_url = S3Url(url=f's3://{self.bucket}/{self.key}')
        self.assertEqual(s3_url.url, f's3://{self.bucket}/{self.key}')
        self.assertEqual(s3_url.bucket, self.bucket)
        self.assertEqual(s3_url.prefix, self.prefix)
        self.assertEqual(s3_url.key, self.key)

    def test_url_attributes_slash(self) -> None:
        s3_url = S3Url(url=f's3://{self.bucket}/{self.prefix}/')
        self.assertEqual(s3_url.url, f's3://{self.bucket}/{self.prefix}/')
        self.assertEqual(s3_url.bucket, self.bucket)
        self.assertEqual(s3_url.prefix, self.prefix)
        self.assertEqual(s3_url.key, f'{self.prefix}/')

    def test_url_attributes_no_prefix(self) -> None:
        s3_url = S3Url(url=f's3://{self.bucket}/{self.file}')
        self.assertEqual(s3_url.url, f's3://{self.bucket}/{self.file}')
        self.assertEqual(s3_url.bucket, self.bucket)
        self.assertEqual(s3_url.prefix, '')
        self.assertEqual(s3_url.key, self.file)

    def test_repr_of_s3_url(self) -> None:
        self.assertEqual(
            S3Url(url=f's3://{self.bucket}/{self.key}').__repr__(),
            f'S3Url(Bucket={self.bucket}, Key={self.key})'
        )

    def test_join_not_ending_with_slash(self) -> None:
        self.assertEqual(
            S3Url(url=f's3://{self.bucket}/{self.prefix}').join('a', 'b'),
            f's3://{self.bucket}/{self.prefix}/a/b'
        )

    def test_join_ending_with_slash(self) -> None:
        self.assertEqual(
            S3Url(url=f's3://{self.bucket}/{self.prefix}/').join('c'),
            f's3://{self.bucket}/{self.prefix}/c'
        )

    def test_join_no_arguments(self) -> None:
        self.assertEqual(
            S3Url(url=f's3://{self.bucket}/{self.prefix}/').join(),
            f's3://{self.bucket}/{self.prefix}/'
        )

    def test_join_error_when_delimiter_in_argument(self) -> None:
        with self.assertRaisesRegex(S3DelimiterError, 'The S3 URL s3://BUCKET_NAME/TEST/a//b contains //'):
            S3Url(url=f's3://{self.bucket}/{self.prefix}/').join('a/', 'b')
