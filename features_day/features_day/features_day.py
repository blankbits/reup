#!/usr/bin/env python3
"""Generates features that are static for the entire day in CSV format.

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


def get_time_window_df(time_series_df: pd.DataFrame, time_windows: List[int],
                       open_timestamp: float,
                       close_timestamp: float) -> pd.DataFrame:
    """Create data frame with VWAP and volume total for the specified time
    windows after the open and before the close.

    Args:
        time_series_df: Data frame of time series data.
        time_windows: List of time window lengths.
        open_timestamp: Market open epoch timestamp.
        close_timestamp: Market close epoch timestamp.

    Returns:
        Time window data frame.

    """
    open_slice_row = time_series_df.loc[time_series_df['timestamp'] ==
                                        open_timestamp].index[0]
    close_slice_row = time_series_df.loc[time_series_df['timestamp'] ==
                                         close_timestamp].index[0] + 1
    df_columns: List[str] = []
    df_data = []
    for i in time_windows:
        volume_total_open = time_series_df['volume_total'].iloc[
            open_slice_row:(open_slice_row + i)].sum()
        vwap_open = (time_series_df['vwap'].iloc[open_slice_row:
                                                 (open_slice_row + i)] *
                     time_series_df['volume_total'].iloc[open_slice_row:(
                         open_slice_row + i)]).sum() / volume_total_open
        volume_total_close = time_series_df['volume_total'].iloc[(
            close_slice_row - i):close_slice_row].sum()
        vwap_close = (time_series_df['vwap'].iloc[
            (close_slice_row - i):close_slice_row] *
                      time_series_df['volume_total'].iloc[
                          (close_slice_row -
                           i):close_slice_row]).sum() / volume_total_close

        df_columns.append('bid_price_open_' + str(i))
        df_data.append(time_series_df.at[open_slice_row + i - 1, 'bid_price'])
        df_columns.append('bid_size_open_' + str(i))
        df_data.append(time_series_df.at[open_slice_row + i - 1, 'bid_size'])
        df_columns.append('ask_price_open_' + str(i))
        df_data.append(time_series_df.at[open_slice_row + i - 1, 'ask_price'])
        df_columns.append('ask_size_open_' + str(i))
        df_data.append(time_series_df.at[open_slice_row + i - 1, 'ask_size'])
        df_columns.append('last_trade_price_open_' + str(i))
        df_data.append(time_series_df.at[open_slice_row + i - 1,
                                         'last_trade_price'])
        df_columns.append('vwap_open_' + str(i))
        df_data.append(vwap_open)
        df_columns.append('volume_total_open_' + str(i))
        df_data.append(volume_total_open)
        df_columns.append('bid_price_close_' + str(i))
        df_data.append(time_series_df.at[close_slice_row - i, 'bid_price'])
        df_columns.append('bid_size_close_' + str(i))
        df_data.append(time_series_df.at[close_slice_row - i, 'bid_size'])
        df_columns.append('ask_price_close_' + str(i))
        df_data.append(time_series_df.at[close_slice_row - i, 'ask_price'])
        df_columns.append('ask_size_close_' + str(i))
        df_data.append(time_series_df.at[close_slice_row - i, 'ask_size'])
        df_columns.append('last_trade_price_close_' + str(i))
        df_data.append(time_series_df.at[close_slice_row - i,
                                         'last_trade_price'])
        df_columns.append('vwap_close_' + str(i))
        df_data.append(vwap_close)
        df_columns.append('volume_total_close_' + str(i))
        df_data.append(volume_total_close)

    return pd.DataFrame([df_data], columns=df_columns)


def get_output_df(time_series_df: pd.DataFrame, time_windows: List[int],
                  open_timestamp: float, close_timestamp: float,
                  weekday: int) -> pd.DataFrame:
    """Create output data frame from time series data, calculating VWAP and
    volume total for the specified time windows after the open and before the
    close.

    Args:
        time_series_df: Data frame of time series data.
        time_windows: List of time window lengths.
        open_timestamp: Market open epoch timestamp.
        close_timestamp: Market close epoch timestamp.
        weekday: Day of the week where Monday is 0 and Sunday is 6.

    Returns:
        Output data frame.

    """
    volume_price_dicts = time_series_df.loc[
        time_series_df['volume_price_dict'].notna(), 'volume_price_dict']
    high_price = np.finfo(np.float64).min
    low_price = np.finfo(np.float64).max
    for json_str in volume_price_dicts:
        json_dict = json.loads(json_str)
        for price in json_dict.keys():
            high_price = np.max([high_price, np.float64(price)])
            low_price = np.min([low_price, np.float64(price)])

    volume_total_day = time_series_df['volume_total'].sum()
    vwap_day = (time_series_df['vwap'] *
                time_series_df['volume_total']).sum() / volume_total_day

    partial_df = pd.DataFrame([[
        open_timestamp, close_timestamp, weekday, high_price, low_price,
        vwap_day, volume_total_day
    ]],
                              columns=[
                                  'open_timestamp', 'close_timestamp',
                                  'weekday', 'high_price', 'low_price',
                                  'vwap_day', 'volume_total_day'
                              ])
    time_window_df = get_time_window_df(time_series_df, time_windows,
                                        open_timestamp, close_timestamp)
    return pd.concat([partial_df, time_window_df], axis=1)


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
    logger.info(json.dumps(event))

    # Download time series CSV file from S3 and load into data frame.
    local_path = reup_utils.download_s3_object(event['s3_bucket'],
                                               event['s3_key_input'])
    with gzip.open(local_path, 'rb') as gzip_file:
        time_series_df = pd.read_csv(gzip_file,
                                     dtype={
                                         'timestamp': 'float64',
                                         'bid_price': 'float64',
                                         'bid_size': 'int64',
                                         'ask_price': 'float64',
                                         'ask_size': 'int64',
                                         'last_trade_price': 'float64',
                                         'vwap': 'float64',
                                         'volume_price_dict': 'string',
                                         'volume_total': 'int64',
                                         'volume_aggressive_buy': 'int64',
                                         'volume_aggressive_sell': 'int64',
                                         'message_count_quote': 'int64',
                                         'message_count_trade': 'int64'
                                     })

    # Parse event params, then create and upload output data frame.
    date_str, _ = event['s3_key_input'].split('/')[-3:-1]
    weekday = datetime.datetime.strptime(date_str, '%Y-%m-%d').weekday()
    open_timestamp = np.max([
        reup_utils.get_timestamp(date_str, event['open_time'],
                                 event['time_zone']),
        time_series_df.at[0, 'timestamp']
    ])
    close_timestamp = np.min([
        reup_utils.get_timestamp(date_str, event['close_time'],
                                 event['time_zone']),
        time_series_df.at[time_series_df.shape[0] - 1, 'timestamp']
    ])
    output_df = get_output_df(time_series_df, event['time_windows'],
                              open_timestamp, close_timestamp, weekday)
    reup_utils.upload_s3_object(
        event['s3_bucket'], event['s3_key_output'],
        gzip.compress(output_df.to_csv(index=False).encode()))
