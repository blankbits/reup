#!/usr/bin/env python3
"""Common utility functions.

"""
import json
import logging
import logging.config
import os
import sys
import time
from typing import Callable, Dict, List
import uuid

import boto3
import botocore
import pandas as pd
import yaml


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


class LambdaInvoke():
    """Invokes a Lambda function async for specified dates and symbols.

    Implements common functionality for looping through through dates and
    symbols, allowing arbitrary numbers of Lambda function invocations to be
    triggered in batches.

    """
    @staticmethod
    def _get_lambda_payload_default(config: dict, lambda_event: dict,
                                    date: str, symbol: str) -> bytes:
        """Build the Lambda payload for a function invocation.

        Args:
            config: Configuration for the Lambda invoke script.
            lambda_event: Template Lambda event to be populated.
            date: Date for this invocation in YYYY-MM-DD format.
            symbol: Symbol for this invocation.

        Returns:
            JSON bytes to be used as Lambda payload.

        """
        payload = lambda_event.copy()
        payload['dates'] = [date]
        payload['symbols'] = [symbol]
        payload['download_location'] = config['download_location']
        return json.dumps(payload).encode()

    @staticmethod
    def _get_pending_invocations_default(
            config: dict, lambda_event: dict,
            date_symbol_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """Find pending Lambda invocations by checking whether output S3 objects
        exist for the given dates, symbols, and other parameters. Returns the
        dates and symbols which are missing S3 objects.

        Args:
            config: Configuration for the Lambda invoke script.
            lambda_event: Template Lambda event to be populated.
            date_symbol_dict: Dict with date keys and symbol list values.

        Returns:
            Dict with date keys and symbol list values.

        """
        s3_bucket = config['download_location'].split('/')[0]
        s3_key_prefix = '/'.join(config['download_location'].split('/')[1:])
        s3_keys = set(get_s3_keys(s3_bucket, s3_key_prefix))

        pending_date_symbol_dict: Dict[str, List[str]] = {}
        for date in sorted(date_symbol_dict.keys()):
            for symbol in sorted(date_symbol_dict[date]):
                s3_key_quotes = '/'.join([
                    s3_key_prefix, date, symbol,
                    lambda_event['quotes_csv_filename']
                ])
                s3_key_trades = '/'.join([
                    s3_key_prefix, date, symbol,
                    lambda_event['trades_csv_filename']
                ])
                if (s3_key_quotes not in s3_keys
                        or s3_key_trades not in s3_keys):
                    if date in pending_date_symbol_dict:
                        if pending_date_symbol_dict[date][-1] != symbol:
                            pending_date_symbol_dict[date].append(symbol)
                    else:
                        pending_date_symbol_dict[date] = [symbol]

        return pending_date_symbol_dict

    def get_pending_invocations(
            self, date_symbol_dict: Dict[str,
                                         List[str]]) -> Dict[str, List[str]]:
        """Calls the corresponding static function with values from this object
        instance. This is a convenience to provide a cleaner API.

        Args:
            date_symbol_dict: Dict with date keys and symbol list values.

        Returns:
            Dict with date keys and symbol list values.

        """
        return self._get_pending_invocations(self._config, self._lambda_event,
                                             date_symbol_dict)

    def __init__(
        self,
        config_file: str,
        lambda_event_file: str,
        get_lambda_payload: Callable[
            [dict, dict, str, str],
            bytes] = _get_lambda_payload_default.__func__,  # type: ignore
        get_pending_invocations: Callable[
            [dict, dict, Dict[str, List[str]]],
            Dict[str, List[str]]] = _get_pending_invocations_default.
        __func__  # type: ignore
    ):
        """Load configuration files and set up object behavior.

        Args:
            config_file: Path to YAML file which determines batching and other
                behavior.
            lambda_event_file: Path to JSON file which is a template Lambda
                payload whose values may be overwritten.
            get_lambda_payload (optional): Function to generate Lambda payloads.
            get_pending_invocations (optional): Function to find dates and
                symbols representing pending Lambda invocations.

        """
        # Load config YAML file and Lambda event JSON file.
        with open(config_file, 'r') as f:
            self._config = yaml.safe_load(f.read())
        with open(lambda_event_file, 'r') as f:
            self._lambda_event = json.load(f)

        # Set functions for generating Lambda payloads and determining pending
        # invocations.
        self._get_lambda_payload = get_lambda_payload
        self._get_pending_invocations = get_pending_invocations

        # Initialize logger.
        with open(self._config['logging_config'], 'r') as f:
            logging.config.dictConfig(yaml.safe_load(f.read()))

    def run(self, date_symbol_dict: Dict[str, List[str]]) -> None:
        """Execute the main logic to invoke Lambda function.

        Args:
            date_symbol_dict: Dict with date keys and symbol list values.

        """
        logger = logging.getLogger(__name__)

        # Process each date and symbol in sorted order.
        client = boto3.client('lambda')
        event_count = 0
        for date in sorted(date_symbol_dict.keys()):
            for symbol in sorted(date_symbol_dict[date]):
                # Invoke Lambda function async.
                logger.info('Invoking Lambda function async | %s',
                            'date:{}, symbol:{}'.format(date, symbol))
                response = client.invoke(
                    FunctionName=self._config['lambda_function'],
                    InvocationType='Event',
                    Payload=self._get_lambda_payload(self._config,
                                                     self._lambda_event, date,
                                                     symbol))

                # Exit if invoke is unsuccessful.
                if response['ResponseMetadata']['HTTPStatusCode'] != 202:
                    logger.error('Lambda invoke failed')
                    logger.error(json.dumps(response))
                    sys.exit(1)

                # Ensure the max number of concurrent events isn't exceeded.
                event_count += 1
                if event_count == self._config['max_event_count']:
                    logger.info(
                        'Sleeping | %s',
                        'event_count:{}, sleep_seconds:{}'.format(
                            event_count, self._config['sleep_seconds']))
                    time.sleep(self._config['sleep_seconds'])
                    event_count = 0
