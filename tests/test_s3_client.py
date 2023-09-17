# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import os
from typing import List
from unittest import mock

from moto import mock_s3

from simpleboto import S3Client, S3Url
from tests.base_test import BaseTest, OS_ENVIRON


@mock_s3
class TestS3Client(BaseTest):
    def setUp(self) -> None:
        super().setUp()

        self.bucket_name = 'test-bucket'

        with mock.patch.dict(OS_ENVIRON, self.env_vars):
            self.s3_client = S3Client(region_name=os.getenv('REGION'))
            self._set_up_s3(bucket_name=self.bucket_name)

    def tearDown(self) -> None:
        super().tearDown()
        self._tear_down_s3()

    def _upload_to_s3(self) -> None:
        self.bucket.put_object(Body=b'a', Key='prefix1/file1')
        self.bucket.put_object(Body=b'ab', Key='prefix1/file2')
        self.bucket.put_object(Body=b'abc', Key='prefix1/file3')
        self.bucket.put_object(Body=b'abcd', Key='prefix2/file4')

    def test_list_without_meta(self) -> None:
        self._upload_to_s3()

        objects = self.s3_client.list(
            S3Url(bucket=self.bucket_name, prefix='prefix1')
        )

        self.assertEqual(objects, [
            S3Url(bucket=self.bucket_name, key='prefix1/file1'),
            S3Url(bucket=self.bucket_name, key='prefix1/file2'),
            S3Url(bucket=self.bucket_name, key='prefix1/file3'),
        ])

    def test_list_with_meta(self) -> None:
        self._upload_to_s3()

        objects: List[dict] = self.s3_client.list(
            S3Url(bucket=self.bucket_name, prefix='prefix2'),
            with_meta=True
        )

        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0]['Key'], 'prefix2/file4')
        self.assertTrue(all(i in objects[0].keys() for i in ['LastModified', 'ETag', 'Size', 'StorageClass']))

    def test_list_no_objects(self) -> None:
        self.assertEqual(
            self.s3_client.list(S3Url(bucket=self.bucket_name, prefix='prefix1')),
            []
        )

    def test_size_multiple_files(self) -> None:
        self._upload_to_s3()

        self.assertEqual(
            self.s3_client.size(s3_url=S3Url(bucket=self.bucket_name, prefix='prefix1')),
            6
        )

    def test_size_one_file(self) -> None:
        self._upload_to_s3()

        self.assertEqual(
            self.s3_client.size(s3_url=S3Url(bucket=self.bucket_name, key='prefix2/file4')),
            4
        )
