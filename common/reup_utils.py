#!/usr/bin/env python3
"""Common utility functions.

"""
import logging
import os
import sys
from typing import Dict, List
import uuid

import boto3
import botocore
import pandas as pd


def download_s3_object(s3_bucket: str,
                       s3_key: str,
                       local_path: str = '') -> str:
    """Download an S3 object. If local path isn't specified, a unique local path
    compatible with Lambda will be generated.

    Args:
        s3_bucket: S3 bucket name for object to download.
        s3_key: S3 key for object to download.
        local_path (optional): Local path.

    Returns:
        Local path for downloaded object.

    """
    logger = logging.getLogger(__name__)
    if not local_path:
        local_path = '/tmp/reup-{}'.format(uuid.uuid4())

    logger.info('Downloading S3 object | %s',
                's3_bucket:{}, s3_key:{}'.format(s3_bucket, s3_key))
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(s3_bucket, s3_key, local_path)
    except botocore.exceptions.ClientError as exception:
        logger.error('S3 object download failed')
        raise exception

    return local_path


def upload_s3_object(s3_bucket: str, s3_key: str, data: bytes) -> None:
    """Upload an S3 object.

    Args:
        s3_bucket: S3 bucket name for object to upload.
        s3_key: S3 key for object to upload.
        data: Binary object data.

    """
    logger = logging.getLogger(__name__)
    logger.info('Uploading S3 object | %s',
                's3_bucket: {}, s3_key: {}'.format(s3_bucket, s3_key))
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(Body=data, Bucket=s3_bucket, Key=s3_key)
    except botocore.exceptions.ClientError as exception:
        logger.error('S3 object upload failed')
        raise exception


def get_s3_keys(s3_bucket: str,
                s3_prefix: str,
                include_folders: bool = False) -> List[str]:
    """Find all the S3 keys in a bucket with a given prefix.

    Args:
        s3_bucket: Name of S3 bucket.
        s3_prefix: Prefix of S3 keys used to filter results.
        include_folders (optional): Determines whether objects with trailing '/'
            in their key are included. Generally these are zero byte
            placeholders which can interfere with behavior.

    Returns:
        List of S3 keys.

    """
    logger = logging.getLogger(__name__)
    s3_client = boto3.client('s3')
    s3_keys: List[str] = []
    continuation_token = ''
    while True:
        logger.info(
            'Fetching S3 object list | %s',
            's3_bucket:{}, s3_prefix:{}, continuation_token:{}'.format(
                s3_bucket, s3_prefix, continuation_token))
        if continuation_token == '':
            response = s3_client.list_objects_v2(Bucket=s3_bucket,
                                                 Prefix=s3_prefix)
        else:
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=s3_prefix,
                ContinuationToken=continuation_token)

        if response['KeyCount'] > 0:
            for item in response['Contents']:
                if not item['Key'].endswith('/') or include_folders:
                    s3_keys.append(item['Key'])

        if response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
        else:
            break

    return s3_keys


def get_date_symbol_dict(s3_keys: List[str],
                         s3_prefix: str) -> Dict[str, List[str]]:
    """Parse S3 keys to find the set of unique date and symbol pairs.

    Args:
        s3_keys: List of S3 keys.
        s3_prefix: Prefix preceding the date and symbol in S3 keys.

    Returns:
        Dict with date keys and symbol list values.

    """
    s3_keys_sorted = s3_keys.copy()
    s3_keys_sorted.sort()
    date_symbol_dict: Dict[str, List[str]] = {}
    for key in s3_keys_sorted:
        date, symbol = key.replace(s3_prefix, '').split('/')[:2]
        if date in date_symbol_dict:
            if date_symbol_dict[date][-1] != symbol:
                date_symbol_dict[date].append(symbol)
        else:
            date_symbol_dict[date] = [symbol]

    return date_symbol_dict


class Universe():
    """A universe of symbols which changes over time.

    Each change is represented by an S3 object whose key ends in YYYY-MM-DD.csv
    format. The constituents of the universe on any given date are contained in
    the most recent CSV file on or prior to that date.

    """
    def __init__(self, s3_bucket: str, s3_prefix: str):
        """Load data from S3 into memory once at initialization.

        Args:
            s3_bucket: S3 bucket name.
            s3_prefix: S3 prefix preceding the universe CSV files and no other
                objects.

        """
        self._dates_sorted: List[str] = []
        self._dates_dfs: Dict[str, pd.DataFrame] = {}

        s3_keys = get_s3_keys(s3_bucket, s3_prefix)
        for key in s3_keys:
            date = key[-14:-4]
            self._dates_sorted.append(date)

            local_path = download_s3_object(s3_bucket, key)
            with open(local_path, 'rb') as f:
                self._dates_dfs[date] = pd.read_csv(f)

            os.remove(local_path)

        self._dates_sorted.sort()

    def _get_most_recent_universe_date(self, date: str) -> str:
        """Find the most recent universe date on or prior to given date.

        Args:
            date: YYYY-MM-DD formatted date.

        Returns:
            YYYY-MM-DD formatted date.

        """
        logger = logging.getLogger(__name__)
        for i in range(len(self._dates_sorted) - 1, -1, -1):
            if self._dates_sorted[i] <= date:
                return self._dates_sorted[i]

        logger.error('No universe date exists on or prior to given date')
        sys.exit(1)

    def get_symbol_df(self, date: str) -> pd.DataFrame:
        """Return dataframe of universe constituents on a given date.

        Args:
            date: YYYY-MM-DD formatted date.

        Returns:
            Dataframe.

        """
        universe_date = self._get_most_recent_universe_date(date)
        return self._dates_dfs[universe_date]

    def get_symbol_list(self, date: str) -> List[str]:
        """Return symbol list of universe constituents on a given date.

        Args:
            date: YYYY-MM-DD formatted date.

        Returns:
            List of str.

        """
        return self.get_symbol_df(date)['symbol'].tolist()
