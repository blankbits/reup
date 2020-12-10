#!/usr/bin/env python3
"""Generates features from time series data for a single symbol on a single day,
and saves in CSV format.

"""
import gzip
import json
import logging
import logging.config
from typing import List

import numpy as np
import pandas as pd

import reup_utils


def get_output_df(time_series_df: pd.DataFrame,
                  time_windows: List[int]) -> pd.DataFrame:
    """Create output data frame from time series data, calculating features for
    trailing time windows using the provided lengths.

    Args:
        time_series_df: Data frame of time series data.
        time_windows: List of time window lengths.

    Returns:
        Output data frame.

    """
    logger = logging.getLogger(__name__)

    # Init output data frame with same number of rows as time series data frame.
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
            'bid_size_median_' + str(i):
            pd.Series([], dtype='Int64'),
            'ask_size_median_' + str(i):
            pd.Series([], dtype='Int64'),
            'bid_ask_spread_median_' + str(i):
            pd.Series([], dtype='float64'),
            'vwap_' + str(i):
            pd.Series([], dtype='float64'),
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

    # # Create temporary data frame to make calculations easier.
    # temp_df = pd.DataFrame({
    #     'timestamp':
    #     pd.Series(time_series_df['timestamp'], dtype='float64'),
    #     'high_price':
    #     pd.Series([], dtype='float64'),
    #     'low_price':
    #     pd.Series([], dtype='float64'),
    #     'bid_ask_size':
    #     pd.Series([], dtype='Int64'),
    #     'bid_ask_spread':
    #     pd.Series([], dtype='float64'),
    #     # 'bid_ask_size':
    #     # pd.Series([], dtype='Int64'),

    # })

    # Populate values cumulative for the whole day.
    logger.info('Populating values cumulative for the whole day')
    high_price_day = np.finfo(np.float64).min
    low_price_day = np.finfo(np.float64).max
    for i in range(len(output_df)):
        if pd.notna(time_series_df.at[i, 'volume_price_dict']):
            for price in json.loads(
                    time_series_df.at[i, 'volume_price_dict']).keys():
                high_price_day = np.max([high_price_day, np.float64(price)])
                low_price_day = np.min([low_price_day, np.float64(price)])

        if high_price_day != np.finfo(np.float64).min:
            output_df.at[i, 'high_price_day'] = high_price_day
            output_df.at[i, 'low_price_day'] = low_price_day

    output_df['volatility_day'] = time_series_df['last_trade_price'].expanding(
    ).std().values
    output_df['vwap_day'] = (
        (time_series_df['vwap'] * time_series_df['volume_total']).cumsum() /
        time_series_df['volume_total'].cumsum()).values
    output_df['volume_total_day'] = time_series_df['volume_total'].cumsum(
    ).values
    output_df['volume_aggressive_buy_day'] = time_series_df[
        'volume_aggressive_buy'].cumsum().values
    output_df['volume_aggressive_sell_day'] = time_series_df[
        'volume_aggressive_sell'].cumsum().values
    output_df['message_count_quote_day'] = time_series_df[
        'message_count_quote'].cumsum().values
    output_df['message_count_trade_day'] = time_series_df[
        'message_count_trade'].cumsum().values

    # Populate values for time windows. Note that rolling method returns floats,
    # so need to cast to Int64 where that is desired.
    for time_window in time_windows:
        logger.info('Populating values for time window | %s',
                    'time_window: {}'.format(time_window))

        # 'high_price_' + str(i):
        # pd.Series([], dtype='float64'),

        # 'low_price_' + str(i):
        # pd.Series([], dtype='float64'),

        output_df['volatility_' + str(time_window)] = time_series_df[
            'last_trade_price'].rolling(time_window).std().values
        output_df['moving_average_' + str(time_window)] = time_series_df[
            'last_trade_price'].rolling(time_window).mean().values
        output_df[
            'moving_average_weighted_' +
            str(time_window)] = time_series_df['last_trade_price'].rolling(
                time_window).apply(lambda x, y=time_window: np.dot(
                    x, np.arange(1, y + 1)) / np.arange(1, y + 1).sum(),
                                   raw=True).values
        output_df['bid_size_median_' +
                  str(time_window)] = time_series_df['bid_size'].rolling(
                      time_window).median().astype('Int64').values
        output_df['ask_size_median_' +
                  str(time_window)] = time_series_df['ask_size'].rolling(
                      time_window).median().astype('Int64').values

        # Y 'bid_ask_spread_median_' + str(i):
        # pd.Series([], dtype='float64'),

        # Y 'vwap_' + str(i):
        # pd.Series([], dtype='float64'),

        output_df['volume_total_' +
                  str(time_window)] = time_series_df['volume_total'].rolling(
                      time_window).sum().astype('Int64').values
        output_df['volume_aggressive_buy_' + str(
            time_window)] = time_series_df['volume_aggressive_buy'].rolling(
                time_window).sum().astype('Int64').values
        output_df['volume_aggressive_sell_' + str(
            time_window)] = time_series_df['volume_aggressive_sell'].rolling(
                time_window).sum().astype('Int64').values
        output_df[
            'message_count_quote_' +
            str(time_window)] = time_series_df['message_count_quote'].rolling(
                time_window).sum().astype('Int64').values
        output_df[
            'message_count_trade_' +
            str(time_window)] = time_series_df['message_count_trade'].rolling(
                time_window).sum().astype('Int64').values

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
