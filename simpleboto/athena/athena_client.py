# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import Optional

import boto3

from simpleboto.athena.utils.schema import Schema
from simpleboto.boto3_base import Boto3Base


class AthenaClient(Boto3Base):
    """
    Wrapper for the boto3 Athena client.
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
        super().__init__('athena', region_name, boto3_session)
        self.athena = self.client

    def get_create_table(
        self,
        schema: Schema
    ) -> str:
        """
        Function to return a CREATE TABLE query based on the input Schema.

        :param schema: the Schema class used to generate the information needed for the query
        """
        # validate Schema has all relevant fields
        # input/output schema are required, but we need...
        # EXTERNAL_LOCATION - bucket + prefix

        template = """CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{name} ()"""

        pass
