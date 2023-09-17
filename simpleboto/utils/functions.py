# -*- coding: utf-8 -*-
"""
(c) Charlie Collier, all rights reserved
"""

from typing import List


def get_provided_parameters(
    parameter_dict: dict
) -> List[str]:
    """
    Function to return a list of the input variables which were provided.

    :param parameter_dict: dictionary with key-values 'parameterName' and 'parameterValue'
    """
    non_null_params = {k: v for k, v in parameter_dict.items() if v}

    return list(non_null_params.keys())
