#!/usr/bin/env python3
"""Generates time series data in CSV format from downloaded Polygon tick data.

"""
import gzip
import logging
import logging.config
import math
import sys
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
        sample_seconds: Period at which to sample data.

    Returns:
        Time series data frame.

    """
    logger = logging.getLogger(__name__)

    # Initialize empty data frame with a row for each time series period. For
    # integer fields, Int64 is used instead of int64 since it is nullable.
    start_time = math.ceil(quotes_df.loc[0, 'sip_timestamp'] / 10.0**9)
    end_time = math.ceil(quotes_df.loc[len(quotes_df) - 1, 'sip_timestamp'] /
                         10.0**9)
    timestamp_values = np.linspace(start_time, end_time,
                                   int(np.round(end_time - start_time + 1.0)))
    # timestamp_values = timestamp_values[
    #     [i % sample_seconds == 0 for i in range(len(timestamp_values))]]
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

    # Populate dataframe.
    quotes_row = 0
    trades_row = 0
    last_trade_price = pd.NA
    for i in range(len(seconds_df)):
        current_timestamp = seconds_df.loc[i, 'timestamp']
        while quotes_df.loc[quotes_row,
                            'sip_timestamp'] / 10.0**9 <= current_timestamp:
            # TODO: Add aggregation here.
            if quotes_row < len(quotes_df) - 1:
                quotes_row += 1
            else:
                break

        while trades_df.loc[trades_row,
                            'sip_timestamp'] / 10.0**9 <= current_timestamp:
            last_trade_price = trades_df.loc[trades_row, 'price']
            if trades_row < len(trades_df) - 1:
                trades_row += 1
            else:
                break

        seconds_df.loc[i, 'bid_price'] = quotes_df.loc[quotes_row, 'bid_price']
        seconds_df.loc[i, 'bid_size'] = quotes_df.loc[quotes_row, 'bid_size']
        seconds_df.loc[i, 'ask_price'] = quotes_df.loc[quotes_row, 'ask_price']
        seconds_df.loc[i, 'ask_size'] = quotes_df.loc[quotes_row, 'ask_size']
        seconds_df.loc[i, 'last_trade_price'] = last_trade_price

    return seconds_df


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

    # Copy quotes and trades gzip files locally, and load into data frames.
    logger.info(
        'Downloading gzip files from S3 | %s',
        's3_bucket:{}, s3_key_quotes:{}, s3_key_trades:{}'.format(
            config['s3_bucket'], config['s3_key_quotes'],
            config['s3_key_trades']))
    s3_client = boto3.client('s3')
    quotes_local_path = '/tmp/{}'.format(uuid.uuid4())
    s3_client.download_file(config['s3_bucket'], config['s3_key_quotes'],
                            quotes_local_path)
    with gzip.open(quotes_local_path, 'rb') as gzip_file:
        quotes_df = pd.read_csv(gzip_file)

    trades_local_path = '/tmp/{}'.format(uuid.uuid4())
    s3_client.download_file(config['s3_bucket'], config['s3_key_trades'],
                            trades_local_path)
    with gzip.open(trades_local_path, 'rb') as gzip_file:
        trades_df = pd.read_csv(gzip_file)

    # Create time series data frame and save to S3.
    seconds_df = create_seconds_df(quotes_df, trades_df)
    logger.info(
        'Writing S3 object | %s',
        's3_bucket: {}, s3_key: {}'.format(config['s3_bucket'],
                                           config['s3_key_output']))
    try:
        s3_client.put_object(Body=gzip.compress(
            seconds_df.to_csv(index=False).encode()),
                             Bucket=config['s3_bucket'],
                             Key=config['s3_key_output'])
    except botocore.exceptions.ClientError as exception:
        logger.error('S3 object write failed')
        raise exception
