# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Optional, Union, List

import boto3

from simpleboto.s3.s3_url import S3Url


class S3Client:
    """
    Wrapper for the boto3 S3 client.
    """
    def __init__(
        self,
        region_name: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None
    ) -> None:
        """
        :param region_name: the name of the AWS region (if not provided, ensure credentials have been exported)
        :param boto3_session: a provided boto3_session
        """
        kwargs = {}
        if region_name:
            kwargs['region_name'] = region_name

        client = boto3_session.client if boto3_session else boto3.client
        self.s3 = client(service_name='s3', **kwargs)

    def list(
        self,
        s3_url: S3Url,
        with_meta: Optional[bool] = False
    ) -> Union[List[S3Url], List[dict]]:
        """
        Function to return all the objects listed from S3 based on the input URL.

        Required IAM permissions:
            s3:ListBucket

        :param s3_url: the S3Url object for where we want to list the objects
        :param with_meta: whether to return the full metadata (True) or just the list of file locations (False)
            if True, will return a list of dictionaries as per the list_objects_v2 response in boto3
            if False, will return a list of S3Url objects
        """
        paginator = self.s3.get_paginator('list_objects_v2')
        response_iter = paginator.paginate(
            Bucket=s3_url.bucket,
            Prefix=s3_url.key
        )

        output = []
        for ele in response_iter:
            if 'Contents' in ele:
                output.extend(ele['Contents'])

        if not with_meta:
            output = [S3Url(bucket=s3_url.bucket, key=ele['Key']) for ele in output]

        return output

    def size(
        self,
        s3_url: S3Url
    ) -> float:
        """
        Function to return the size of a file/directory in S3.

        Required IAM permissions:
            s3:ListBucket

        :param s3_url: an S3Url object of either a file or directory in S3

        :return: the size of the file/directory in bytes
        """
        objects: List[dict] = self.list(s3_url=s3_url, with_meta=True)

        return sum(ele['Size'] for ele in objects)
