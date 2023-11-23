# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""
import re
import boto3
from typing import Optional, Union, List
from botocore.exceptions import ClientError

from simpleboto.exceptions import KeyNotFound
from simpleboto.boto3_base import Boto3Base
from simpleboto.s3.s3_url import S3Url


class S3Client(Boto3Base):
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
        super().__init__('s3', region_name, boto3_session)
        self.s3 = self.client

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

    def copy_objects(
            self,
            s3_url_src: S3Url,
            s3_url_dst: S3Url,
            regex_pattern: Optional[str] = None,
            rewrite: Optional[bool] = False
    ) -> None:
        """
        copy objects from source url to destination url.
         If source and destination are the same, or the destination exists, exception is raised
        :param s3_url_src: an S3Url object of either a single object or entire prefix in S3 which needs to be copied
        :param s3_url_dst: an S3Url object of directory where source file or prefix needs to be copied into
        :param regex_pattern: regex for glob
        :param rewrite: to rewrite objects in destination
        :return: None
        """
        objects_to_copy = self.list(s3_url_src)

        if regex_pattern:
            objects_to_copy = [key for key in objects_to_copy if re.match(regex_pattern, key)]
        if not objects_to_copy:
            raise KeyNotFound(url=s3_url_src)

        if not rewrite:
            check_dest_file = self.list(s3_url_dst)
            dest_overlap = [obj.key for obj in objects_to_copy if obj in check_dest_file]
            if dest_overlap:
                raise ValueError(f'Keys: {dest_overlap} already in destination: {s3_url_dst}')

        copy_dict = {
            k: S3Url(bucket=s3_url_dst.bucket, key='/'.join([s3_url_dst.prefix, k.key]))
            if s3_url_dst.key.endswith('/')
            else S3Url(bucket=s3_url_dst.bucket, key=s3_url_dst.key)
            if (s3_url_dst.key and len(objects_to_copy) == 1)
            else S3Url(bucket=s3_url_dst.bucket, key=k.key)
            for k in objects_to_copy
        }

        for source, destination in copy_dict.items():
            try:
                self.s3.copy({"Bucket": source.bucket, "Key": source.key}, destination.bucket, destination.key)
            except ClientError as e:
                if e.response['Error']['Code'] == 'InvalidRequest':
                    self.s3.copy({"Bucket": source.bucket, "Key": source.key}, destination.bucket,
                                 destination.key + '_copy')
                else:
                    destination_full = S3Url(bucket=destination.bucket, key=destination.key)
                    raise RuntimeError(f"Error copying {source.key} to {destination_full.url}: {str(e)}")
            except Exception as e:
                destination_full = S3Url(bucket=destination.bucket, key=destination.key)
                raise RuntimeError(f"Error copying {source.key} to {destination_full.url}: {str(e)}")
