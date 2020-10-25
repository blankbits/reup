#!/usr/bin/env python3
# import gzip
import json
import logging
import logging.config
# from typing import Dict
# import uuid

# import boto3
# import botocore
# import numpy as np
# import pandas as pd

import reup_utils


def main_lambda(event: dict, context) -> None:
    """Start execution when running on Lambda.

    Args:
        event: Lambda event provided by environment.
        context: Lambda context provided by environment. This is not used, and
            doesn't have a type hint as type is defined by Lambda.

    """
    # pylint: disable=unused-argument

    # Initialize logger.
    logging.config.dictConfig(event['logging'])
    logger = logging.getLogger(__name__)

    # TODO: Add main logic here.
    print(json.dumps(event))
    print(event)
