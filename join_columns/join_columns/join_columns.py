#!/usr/bin/env python3
"""Joins columns of data together in CSV format.

"""
import datetime
import gzip
import json
import logging
import logging.config
from typing import List

import numpy as np
import pandas as pd

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
    # logger = logging.getLogger(__name__)

    # # Download time series CSV file from S3 and load into data frame.
    # local_path = reup_utils.download_s3_object(event['s3_bucket'],
    #                                            event['s3_key_input'])
    # with gzip.open(local_path, 'rb') as gzip_file:
    #     time_series_df = pd.read_csv(gzip_file,
    #                                  dtype={
    #                                      'timestamp': 'float64',
    #                                      'bid_price': 'float64',
    #                                      'bid_size': 'int64',
    #                                      'ask_price': 'float64',
    #                                      'ask_size': 'int64',
    #                                      'last_trade_price': 'float64',
    #                                      'vwap': 'float64',
    #                                      'volume_price_dict': 'string',
    #                                      'volume_total': 'int64',
    #                                      'volume_aggressive_buy': 'int64',
    #                                      'volume_aggressive_sell': 'int64',
    #                                      'message_count_quote': 'int64',
    #                                      'message_count_trade': 'int64'
    #                                  })

    # # Parse event params, then create and upload output data frame.
    # date_str, _ = event['s3_key_input'].split('/')[-3:-1]
    # weekday = datetime.datetime.strptime(date_str, '%Y-%m-%d').weekday()
    # open_timestamp = np.max([
    #     reup_utils.get_timestamp(date_str, event['open_time'],
    #                              event['time_zone']),
    #     time_series_df.at[0, 'timestamp']
    # ])
    # close_timestamp = np.min([
    #     reup_utils.get_timestamp(date_str, event['close_time'],
    #                              event['time_zone']),
    #     time_series_df.at[time_series_df.shape[0] - 1, 'timestamp']
    # ])
    # output_df = get_output_df(time_series_df, event['time_windows'],
    #                           open_timestamp, close_timestamp, weekday)
    # reup_utils.upload_s3_object(
    #     event['s3_bucket'], event['s3_key_output'],
    #     gzip.compress(output_df.to_csv(index=False).encode()))
