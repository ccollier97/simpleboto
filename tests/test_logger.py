# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import logging
import os
import re

from simpleboto.logs.logger import CLogger
from tests.base_test import BaseTest


class TestLogger(BaseTest):
    def setUp(self) -> None:
        super().setUp()

        self.log_test_path = os.path.join(self.tmp_dir, 'test_logs.txt')

    def test_debug_logging(self) -> None:
        log_instance = CLogger(__name__, logging_level=logging.DEBUG)
        with self.assertLogs(__name__, level=logging.DEBUG):
            log_instance.debug(message="DEBUG LOG")

    def test_info_logging(self) -> None:
        log_instance = CLogger(__name__, logging_level=logging.INFO)
        with self.assertLogs(__name__, level=logging.INFO):
            log_instance.info(message="INFO LOG")

    def test_error_logging(self) -> None:
        log_instance = CLogger(__name__, logging_level=logging.ERROR)
        with self.assertLogs(__name__, level=logging.ERROR):
            log_instance.error(message="ERROR LOG")

    def test_warning_logging(self) -> None:
        log_instance = CLogger(__name__, logging_level=logging.WARNING)
        with self.assertLogs(__name__, level=logging.WARNING):
            log_instance.warning(message="WARNING LOG")

    def test_logging_lower_level(self) -> None:
        log_instance = CLogger(__name__, logging_level=logging.WARNING)
        with self.assertLogs(__name__, level=logging.WARNING) as logs:
            log_instance.info(message="INFO LOG")
            log_instance.error(message="ERROR LOG")

        self.assertEqual(logs.output[0], f'ERROR:{__name__}:ERROR LOG')

    def test_log_to_file(self) -> None:
        log_instance = CLogger(__name__, log_file_path=self.log_test_path)

        log_instance.info(message="TEST INFO LOG")
        log_instance.warning(message="TEST WARNING LOG")

        with open(self.log_test_path) as f:
            expected_logs = f.read()

        ts_regex = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}'
        ignore_time_logs = [re.sub(ts_regex, '', i) for i in expected_logs.split('\n')][:-1]
        self.assertListEqual(ignore_time_logs, [
            f'{__name__} |  | INFO     | TEST INFO LOG',
            f'{__name__} |  | WARNING  | TEST WARNING LOG'
        ])
