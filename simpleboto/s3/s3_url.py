# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Optional

from simpleboto.exceptions import (
    S3DelimiterError,
    NoParameterError
)


class S3Url(str):
    """
    Class for S3 URLs; can either be specified by the s3:// URL or from the bucket and prefix.
    For example, the URL s3://bucket-name/folder1/file.ext has:
        - Bucket = bucket-name
        - Key = folder1/file.ext
        - Prefix = folder1
    """
    def __new__(
        cls,
        url: Optional[str] = None,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> str:
        """
        :param url: the full S3 URL; can be None if providing bucket/key/prefix
        :param bucket: the S3 bucket name; ignored if url is provided
        :param key: the S3 key
        :param prefix: the S3 prefix; ignored if key is provided
        """
        if url:
            return super().__new__(cls, cls.__validate_url(url))
        elif bucket:
            if key:  # takes precedence
                url = f"{bucket}/{key}"
            elif prefix:
                url = f"{bucket}/{prefix.rstrip('/')}/"
            else:
                url = bucket
            return super().__new__(cls, cls.__validate_url(url))
        elif key or prefix:
            raise NoParameterError(param='bucket', context=cls.__name__)
        else:
            raise NoParameterError(param='ANY', context=cls.__name__)

    @staticmethod
    def __validate_url(
        url: str
    ) -> str:
        """
        Function to validate the S3 URL provided as input to the S3Url class.
        """
        if not url.startswith('s3://'):
            url = f's3://{url}'
        if '//' in url.replace('s3://', ''):
            raise S3DelimiterError(url)

        return url

    def __init__(
        self,
        *_,
        **__
    ) -> None:
        super().__init__()

        self._url = self
        stripped_protocol = self.replace('s3://', '')

        self.bucket = stripped_protocol.split('/')[0]
        self.key = '/'.join(stripped_protocol.split('/')[1:])
        self.prefix = '/'.join(stripped_protocol.split('/')[1:-1])

    @property
    def url(self) -> str:
        return self._url.__str__()

    def __repr__(
        self
    ) -> str:
        return f"S3Url(Bucket={self.bucket}, Key={self.key})"

    def join(
        self,
        *paths
    ):
        """
        Function to append paths to an S3Url.

        :param paths: variable number of arguments to join
        """
        joined_paths = '/'.join(paths)
        delimiter = '' if self.url.endswith('/') else '/'
        url = f'{self.url}{delimiter}{joined_paths}'

        return S3Url(url=url)
