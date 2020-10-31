#!/usr/bin/env python3
"""Generates features that are static for the entire day in CSV format.

"""
import datetime
import gzip
import json
import logging
import logging.config

import numpy as np
import pandas as pd

import reup_utils


def get_output_df(time_series_df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """Create output data frame from time series data and date.

    Args:
        time_series_df: Data frame of time series data.
        date_str: Date string in YYYY-MM-DD format.

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

    volume_total = time_series_df['volume_total'].sum()
    vwap = (time_series_df['vwap'] *
            time_series_df['volume_total']).sum() / volume_total
    weekday = datetime.datetime.strptime(date_str, '%Y-%m-%d').weekday()

    return pd.DataFrame(
        [[high_price, low_price, vwap, volume_total, weekday]],
        columns=['high_price', 'low_price', 'vwap', 'volume_total', 'weekday'])


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
    date_str, _ = event['s3_key_input'].split('/')[-3:-1]
    output_df = get_output_df(time_series_df, date_str)
    reup_utils.upload_s3_object(
        event['s3_bucket'], event['s3_key_output'],
        gzip.compress(output_df.to_csv(index=False).encode()))
