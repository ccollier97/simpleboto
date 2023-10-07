# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

import logging
from typing import Optional


class CLogger:
    """
    Class for a custom logging tool.
    """
    def __init__(
        self,
        logger_name: str,
        logging_level: Optional = logging.INFO,
        log_file_path: Optional[str] = None
    ) -> None:
        """
        :param logger_name: the name of the logger instance
        :param logging_level: the base level of logging for this logger; one of DEBUG, INFO, WARNING or ERROR
        :param log_file_path: the location to save the logs to, if required
        """
        self.logger_name = logger_name

        log_instance = logging.getLogger(self.logger_name)
        log_instance.handlers.clear()
        log_instance.propagate = False
        log_instance.setLevel(logging_level)

        log_formatter = logging.Formatter(
            fmt='%(name)s | %(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_formatter)
        log_instance.addHandler(stream_handler)

        if log_file_path:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(log_formatter)
            log_instance.addHandler(file_handler)

        self.log_instance = log_instance

    def debug(
        self,
        message: str
    ) -> None:
        """
        Function to log any DEBUG messages.

        :param message: the message to log
        """
        self.log_instance.debug(msg=message)

    def info(
        self,
        message: str
    ) -> None:
        """
        Function to log any INFO messages.

        :param message: the message to log
        """
        self.log_instance.info(msg=message)

    def warning(
        self,
        message: str
    ) -> None:
        """
        Function to log any WARNING messages.

        :param message: the message to log
        """
        self.log_instance.warning(msg=message)

    def error(
        self,
        message: str
    ) -> None:
        """
        Function to log any ERROR messages.

        :param message: the message to log
        """
        self.log_instance.error(msg=message)
