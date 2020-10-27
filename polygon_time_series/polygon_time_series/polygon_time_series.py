#!/usr/bin/env python3
"""Generates time series data in CSV format from downloaded Polygon tick data.

"""
import gzip
import json
import logging
import logging.config
from typing import Dict

import numpy as np
import pandas as pd

import reup_utils


def init_seconds_df(quotes_df: pd.DataFrame) -> pd.DataFrame:
    """Initialize empty data frame with a row for each time series period. For
    integer fields, Int64 is used instead of int64 since it is nullable.

    Args:
        quotes_df: Data frame of quote messages.

    Returns:
        Empty time series data frame.

    """
    start_time = np.ceil(quotes_df.at[0, 'sip_timestamp'] / 10.0**9)
    end_time = np.ceil(quotes_df.at[len(quotes_df) - 1, 'sip_timestamp'] /
                       10.0**9)
    timestamp_values = np.linspace(start_time, end_time,
                                   int(np.round(end_time - start_time + 1.0)))
    seconds_df = pd.DataFrame({
        'timestamp':
        pd.Series(timestamp_values, dtype='float64'),
        'bid_price':
        pd.Series([], dtype='float64'),
        'bid_size':
        pd.Series([], dtype='Int64'),
        'ask_price':
        pd.Series([], dtype='float64'),
        'ask_size':
        pd.Series([], dtype='Int64'),
        'last_trade_price':
        pd.Series([], dtype='float64'),
        'vwap':
        pd.Series([], dtype='float64'),
        'volume_price_dict':
        pd.Series([], dtype='object'),
        'volume_total':
        pd.Series([], dtype='Int64'),
        'volume_aggressive_buy':
        pd.Series([], dtype='Int64'),
        'volume_aggressive_sell':
        pd.Series([], dtype='Int64'),
        'message_count_quote':
        pd.Series([], dtype='Int64'),
        'message_count_trade':
        pd.Series([], dtype='Int64')
    })

    return seconds_df


def get_seconds_df(quotes_df: pd.DataFrame,
                   trades_df: pd.DataFrame) -> pd.DataFrame:
    """Create time series data frame by sampling from quotes and trades tick
    data once per second, and aggregating volume and message data.

    Args:
        quotes_df: Data frame of quote messages.
        trades_df: Data frame of trade messages.

    Returns:
        Time series data frame.

    """
    # Initialize data frame and outer loop state variables.
    seconds_df = init_seconds_df(quotes_df)
    quotes_row = 0
    trades_row = 0
    last_value = {
        'bid_price': pd.NA,
        'bid_size': pd.NA,
        'ask_price': pd.NA,
        'ask_size': pd.NA,
        'trade_price': pd.NA,
        'trade_size': pd.NA
    }

    # Loop through data frame rows and populate values.
    for i in range(len(seconds_df)):
        current_timestamp = seconds_df.at[i, 'timestamp']
        volume_price_dict: Dict[str, int] = {}
        total = {
            'volume': 0,
            'volume_aggressive_buy': 0,
            'volume_aggressive_sell': 0,
            'price': 0.0,
            'message_count_quote': 0,
            'message_count_trade': 0
        }

        # Loop through quote messages in this period.
        while (quotes_row < len(quotes_df)
               and quotes_df.at[quotes_row, 'sip_timestamp'] / 10.0**9 <=
               current_timestamp):
            total['message_count_quote'] += 1
            quotes_row += 1

        last_value['bid_price'] = quotes_df.at[quotes_row - 1, 'bid_price']
        last_value['bid_size'] = quotes_df.at[quotes_row - 1, 'bid_size'] * 100
        last_value['ask_price'] = quotes_df.at[quotes_row - 1, 'ask_price']
        last_value['ask_size'] = quotes_df.at[quotes_row - 1, 'ask_size'] * 100

        # Loop through trade messages in this period.
        while (trades_row < len(trades_df)
               and trades_df.at[trades_row, 'sip_timestamp'] / 10.0**9 <=
               current_timestamp):
            last_value['trade_price'] = trades_df.at[trades_row, 'price']
            last_value['trade_size'] = trades_df.at[trades_row, 'size']
            total['volume'] += last_value['trade_size']
            total['price'] += last_value['trade_size'] * last_value[
                'trade_price']

            # Populate volume price dict. Need to cast trade sizes to int in
            # order for JSON serialization to work.
            price_key = str(last_value['trade_price'])
            if price_key in volume_price_dict:
                volume_price_dict[price_key] += int(last_value['trade_size'])
            else:
                volume_price_dict[price_key] = int(last_value['trade_size'])

            if (last_value['ask_price'] is not pd.NA
                    and last_value['trade_price'] >=
                    last_value['ask_price'] - np.finfo(np.float64).eps):
                total['volume_aggressive_buy'] += last_value['trade_size']

            if (last_value['bid_price'] is not pd.NA
                    and last_value['trade_price'] <=
                    last_value['bid_price'] + np.finfo(np.float64).eps):
                total['volume_aggressive_sell'] += last_value['trade_size']

            total['message_count_trade'] += 1
            trades_row += 1

        # Populate data frame values for this row.
        seconds_df.at[i, 'bid_price'] = last_value['bid_price']
        seconds_df.at[i, 'bid_size'] = last_value['bid_size']
        seconds_df.at[i, 'ask_price'] = last_value['ask_price']
        seconds_df.at[i, 'ask_size'] = last_value['ask_size']
        seconds_df.at[i, 'last_trade_price'] = last_value['trade_price']
        if total['volume'] > 0:
            seconds_df.at[i, 'vwap'] = total['price'] / total['volume']
        if volume_price_dict:
            seconds_df.at[i,
                          'volume_price_dict'] = json.dumps(volume_price_dict)
        seconds_df.at[i, 'volume_total'] = total['volume']
        seconds_df.at[i,
                      'volume_aggressive_buy'] = total['volume_aggressive_buy']
        seconds_df.at[
            i, 'volume_aggressive_sell'] = total['volume_aggressive_sell']
        seconds_df.at[i, 'message_count_quote'] = total['message_count_quote']
        seconds_df.at[i, 'message_count_trade'] = total['message_count_trade']

    return seconds_df


def discard_trade_conditions(trades_df: pd.DataFrame,
                             trade_conditions: dict) -> pd.DataFrame:
    """Remove rows from the trades data frame which contain any of the trade
    conditions provided.

    Args:
        trades_df: Data frame of trade messages.
        trade_conditions: Dict with keys representing trade conditions to
            discard and values containing descriptions of the conditions for
            debugging purposes.

    Returns:
        Trades data frame.

    """
    discard_mask = trades_df['conditions'].str.split().fillna(False)
    discard_mask = [
        (any(k in d for k in trade_conditions.keys()) if bool(d) else False)
        for d in discard_mask
    ]
    return trades_df.loc[np.invert(discard_mask), ].reset_index(drop=True)


def main_lambda(event: dict, context) -> None:
    """Start execution when running on Lambda.

    Args:
        event: Lambda event provided by environment.
        context: Lambda context provided by environment. This is not used, and
            doesn't have a type hint as type is defined by Lambda.

    """
    # pylint: disable=unused-argument

    # Load config from Lambda event.
    config = event['polygon_time_series']

    # Initialize logger.
    logging.config.dictConfig(config['logging'])
    # logger = logging.getLogger(__name__)

    # Download quote and trade CSV files from S3 and load into data frames.
    quotes_local_path = reup_utils.download_s3_object(config['s3_bucket'],
                                                      config['s3_key_quotes'])
    with gzip.open(quotes_local_path, 'rb') as gzip_file:
        quotes_df = pd.read_csv(gzip_file)

    trades_local_path = reup_utils.download_s3_object(config['s3_bucket'],
                                                      config['s3_key_trades'])
    with gzip.open(trades_local_path, 'rb') as gzip_file:
        trades_df = pd.read_csv(gzip_file)

    # Discard trades that should be ignored.
    trades_df = discard_trade_conditions(trades_df,
                                         config['discard_trade_conditions'])

    # Create time series data frame and save CSV file to S3.
    seconds_df = get_seconds_df(quotes_df, trades_df)
    reup_utils.upload_s3_object(
        config['s3_bucket'], config['s3_key_output'],
        gzip.compress(seconds_df.to_csv(index=False).encode()))
