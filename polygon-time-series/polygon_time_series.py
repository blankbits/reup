#!/usr/bin/env python3
"""Generates time series data in CSV format from downloaded Polygon tick data.

"""
import gzip
import json
import logging
import logging.config
import math
# import sys
import uuid

import boto3
import botocore
import numpy as np
import pandas as pd


def create_seconds_df(quotes_df: pd.DataFrame,
                      trades_df: pd.DataFrame) -> pd.DataFrame:
    """Create time series data frame by sampling from quotes and trades tick
    data once per second, and aggregating volume and message data.

    Args:
        quotes_df: Data frame of quote messages.
        trades_df: Data frame of trade messages.

    Returns:
        Time series data frame.

    """
    logger = logging.getLogger(__name__)

    # Initialize empty data frame with a row for each time series period. For
    # integer fields, Int64 is used instead of int64 since it is nullable.
    start_time = math.ceil(quotes_df.at[0, 'sip_timestamp'] / 10.0**9)
    end_time = math.ceil(quotes_df.at[len(quotes_df) - 1, 'sip_timestamp'] /
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

    # Populate data frame.
    quotes_row = 0
    trades_row = 0
    last_bid_price = pd.NA
    last_bid_size = pd.NA
    last_ask_price = pd.NA
    last_ask_size = pd.NA
    last_trade_price = pd.NA
    for i in range(len(seconds_df)):
        current_timestamp = seconds_df.at[i, 'timestamp']
        vwap = 0.0
        volume_price_dict = {}
        volume_total = 0
        volume_aggressive_buy = 0
        volume_aggressive_sell = 0
        message_count_quote = 0
        message_count_trade = 0

        # Loop through all quote messages in this period.
        while quotes_df.at[quotes_row,
                           'sip_timestamp'] / 10.0**9 <= current_timestamp:
            message_count_quote += 1
            if quotes_row < len(quotes_df) - 1:
                quotes_row += 1
            else:
                break

        last_bid_price = quotes_df.at[max(0, quotes_row - 1), 'bid_price']
        last_bid_size = quotes_df.at[max(0, quotes_row - 1), 'bid_size'] * 100
        last_ask_price = quotes_df.at[max(0, quotes_row - 1), 'ask_price']
        last_ask_size = quotes_df.at[max(0, quotes_row - 1), 'ask_size'] * 100

        # Loop through all trade messages in this period.
        while trades_df.at[trades_row,
                           'sip_timestamp'] / 10.0**9 <= current_timestamp:
            last_trade_price = trades_df.at[trades_row, 'price']
            last_trade_size = trades_df.at[trades_row, 'size']
            volume_total += last_trade_size
            vwap += last_trade_size * last_trade_price

            # Populate volume price dict. Need to cast trade sizes to int in
            # order for JSON serialization to work.
            price_key = str(last_trade_price)
            if price_key in volume_price_dict:
                volume_price_dict[price_key] += int(last_trade_size)
            else:
                volume_price_dict[price_key] = int(last_trade_size)

            if (last_ask_price is not pd.NA and last_trade_price >=
                    last_ask_price - np.finfo(np.float64).eps):
                volume_aggressive_buy += last_trade_size

            if (last_bid_price is not pd.NA and last_trade_price <=
                    last_bid_price + np.finfo(np.float64).eps):
                volume_aggressive_sell += last_trade_size

            message_count_trade += 1

            if trades_row < len(trades_df) - 1:
                trades_row += 1
            else:
                break

        # Divide sum of trade size * price by total volume to calculate vwap.
        if volume_total > 0:
            vwap /= volume_total
        else:
            vwap = pd.NA

        # Serialize volume_price_dict to CSV-friendly string.
        if volume_price_dict:
            volume_price_dict_str = json.dumps(volume_price_dict,
                                               separators=(',', ':'))
        else:
            volume_price_dict_str = ''

        seconds_df.at[i, 'bid_price'] = last_bid_price
        seconds_df.at[i, 'bid_size'] = last_bid_size
        seconds_df.at[i, 'ask_price'] = last_ask_price
        seconds_df.at[i, 'ask_size'] = last_ask_size
        seconds_df.at[i, 'last_trade_price'] = last_trade_price
        seconds_df.at[i, 'vwap'] = vwap
        seconds_df.at[i, 'volume_price_dict'] = volume_price_dict_str
        seconds_df.at[i, 'volume_total'] = volume_total
        seconds_df.at[i, 'volume_aggressive_buy'] = volume_aggressive_buy
        seconds_df.at[i, 'volume_aggressive_sell'] = volume_aggressive_sell
        seconds_df.at[i, 'message_count_quote'] = message_count_quote
        seconds_df.at[i, 'message_count_trade'] = message_count_trade

    return seconds_df


def download_s3_object(s3_bucket: str, s3_key: str) -> str:
    """Download an S3 object to local storage for this Lambda instance.

    Args:
        s3_bucket: S3 bucket name for object to download.
        s3_key: S3 key for object to download.

    Returns:
        Unique local path for downloaded object.

    """
    logger = logging.getLogger(__name__)
    local_path = '/tmp/{}'.format(uuid.uuid4())
    logger.info('Downloading S3 object | %s',
                's3_bucket:{}, s3_key:{}'.format(s3_bucket, s3_key))
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(s3_bucket, s3_key, local_path)
    except botocore.exceptions.ClientError as exception:
        logger.error('S3 object download failed')
        raise exception

    return local_path


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
    logger = logging.getLogger(__name__)

    # Download quote and trade CSV files from S3 and load into data frames.
    quotes_local_path = download_s3_object(config['s3_bucket'],
                                           config['s3_key_quotes'])
    with gzip.open(quotes_local_path, 'rb') as gzip_file:
        quotes_df = pd.read_csv(gzip_file)

    trades_local_path = download_s3_object(config['s3_bucket'],
                                           config['s3_key_trades'])
    with gzip.open(trades_local_path, 'rb') as gzip_file:
        trades_df = pd.read_csv(gzip_file)

    # Create time series data frame and save CSV file to S3.
    seconds_df = create_seconds_df(quotes_df, trades_df)
    logger.info(
        'Writing S3 object | %s',
        's3_bucket: {}, s3_key: {}'.format(config['s3_bucket'],
                                           config['s3_key_output']))
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(Body=gzip.compress(
            seconds_df.to_csv(index=False).encode()),
                             Bucket=config['s3_bucket'],
                             Key=config['s3_key_output'])
    except botocore.exceptions.ClientError as exception:
        logger.error('S3 object write failed')
        raise exception
