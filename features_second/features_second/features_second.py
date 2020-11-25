#!/usr/bin/env python3
"""Generates features from time series data for a single symbol on a single day,
and saves in CSV format.

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


def get_output_df(time_series_df: pd.DataFrame, time_windows: List[int]) -> pd.DataFrame:
    """Create output data frame from time series data, calculating features for
    trailing time windows using the provided lengths.

    Args:
        time_series_df: Data frame of time series data.
        time_windows: List of time window lengths.

    Returns:
        Output data frame.

    """
    # volume_price_dicts = time_series_df.loc[
    #     time_series_df['volume_price_dict'].notna(), 'volume_price_dict']
    # high_price = np.finfo(np.float64).min
    # low_price = np.finfo(np.float64).max
    # for json_str in volume_price_dicts:
    #     json_dict = json.loads(json_str)
    #     for price in json_dict.keys():
    #         high_price = np.max([high_price, np.float64(price)])
    #         low_price = np.min([low_price, np.float64(price)])

    # volume_total = time_series_df['volume_total'].sum()
    # vwap = (time_series_df['vwap'] *
    #         time_series_df['volume_total']).sum() / volume_total
    # weekday = datetime.datetime.strptime(date_str, '%Y-%m-%d').weekday()

    # return pd.DataFrame(
    #     [[high_price, low_price, vwap, volume_total, weekday]],
    #     columns=['high_price', 'low_price', 'vwap', 'volume_total', 'weekday'])

    # logger = logging.getLogger(__name__)
    # logger.info(str(time_windows))

    output_df = pd.DataFrame({
        'timestamp':
        pd.Series(time_series_df['timestamp'], dtype='float64'),
        'high_price_day':
        pd.Series([], dtype='float64'),
        'low_price_day':
        pd.Series([], dtype='float64'),
        'volatility_day':
        pd.Series([], dtype='float64'),
        'vwap_day':
        pd.Series([], dtype='float64'),
        'volume_total_day':
        pd.Series([], dtype='Int64'),
        'volume_aggressive_buy_day':
        pd.Series([], dtype='Int64'),
        'volume_aggressive_sell_day':
        pd.Series([], dtype='Int64'),
        'message_count_quote_day':
        pd.Series([], dtype='Int64'),
        'message_count_trade_day':
        pd.Series([], dtype='Int64')
    })
    for i in time_windows:
        time_window_df = pd.DataFrame({
            'high_price_' + str(i):
            pd.Series([], dtype='float64'),
            'low_price_' + str(i):
            pd.Series([], dtype='float64'),
            'volatility_' + str(i):
            pd.Series([], dtype='float64'),
            'moving_average_' + str(i):
            pd.Series([], dtype='float64'),
            'moving_average_weighted_' + str(i):
            pd.Series([], dtype='float64'),
            'bid_ask_size_median_' + str(i):
            pd.Series([], dtype='Int64'),
            'bid_ask_spread_median_' + str(i):
            pd.Series([], dtype='float64'),
            'vwap_' + str(i):
            pd.Series([], dtype='float64'),
            'volume_price_dict_' + str(i):
            pd.Series([], dtype='object'),
            'volume_total_' + str(i):
            pd.Series([], dtype='Int64'),
            'volume_aggressive_buy_' + str(i):
            pd.Series([], dtype='Int64'),
            'volume_aggressive_sell_' + str(i):
            pd.Series([], dtype='Int64'),
            'message_count_quote_' + str(i):
            pd.Series([], dtype='Int64'),
            'message_count_trade_' + str(i):
            pd.Series([], dtype='Int64')
        })
        output_df = pd.concat([output_df, time_window_df], axis=1)

    return output_df


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

    # Download time series CSV file from S3 and load into data frame.
    local_path = reup_utils.download_s3_object(event['s3_bucket'],
                                               event['s3_key_input'])
    with gzip.open(local_path, 'rb') as f:
        time_series_df = pd.read_csv(f)

    # Create and upload output data frame.
    output_df = get_output_df(time_series_df, event['time_windows'])
    reup_utils.upload_s3_object(
        event['s3_bucket'], event['s3_key_output'],
        gzip.compress(output_df.to_csv(index=False).encode()))
