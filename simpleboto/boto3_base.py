# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Optional

import boto3


class Boto3Base:
    """
    Wrapper for the boto3 client.
    """
    def __init__(
        self,
        service_name: str,
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

        self.session = boto3_session if boto3_session else boto3.Session()
        self.client = self.session.client(service_name=service_name, **kwargs)
