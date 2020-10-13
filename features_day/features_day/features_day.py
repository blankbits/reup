#!/usr/bin/env python3
# import gzip
# import json
import logging
import logging.config
# from typing import Dict
# import uuid

# import boto3
# import botocore
# import numpy as np
# import pandas as pd


def main_lambda(event: dict, context) -> None:
    # pylint: disable=unused-argument

    # Load config from Lambda event.
    config = event['features_day']

    # Initialize logger.
    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger(__name__)

    # TODO: Add main logic here.
