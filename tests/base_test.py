# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import os
import shutil
from unittest import TestCase

import boto3

OS_ENVIRON = 'os.environ'


class BaseTest(TestCase):
    def setUp(self) -> None:
        self.env_vars = {
            'REGION': 'us-east-1'
        }

        self.test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')

        self.tmp_dir = os.path.join(os.path.dirname(__file__), 'tmp')
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir)

    def _set_up_s3(self, bucket_name: str) -> None:
        self.s3c = boto3.client('s3', region_name=os.getenv('REGION'))
        self.s3r = boto3.resource('s3', region_name=os.getenv('REGION'))
        self.bucket = self.s3r.Bucket(bucket_name)
        self.bucket.create()

    def _tear_down_s3(self) -> None:
        self.bucket.objects.all().delete()
        self.bucket.delete()
