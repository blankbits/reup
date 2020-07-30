#!/usr/bin/env python3
"""Contains a main function which downloads historical quote and trade data from
Polygon, does some basic validation, and dumps both the raw API responses and
CSV formatted data to disk.

It defaults to using the config specified in polygon_download_config.yaml, but
this can be overridden via a command line arg.

Example:
    ./polygon_download.py --config_file custom_config.yaml

"""
import argparse
import datetime
import enum
import gzip
import io
import logging
import logging.config
import os
import pickle
import sys
import threading

import boto3
import botocore
import pandas as pd
import polygon
import pytz
import yaml


class EnvironmentType(enum.Enum):
    """Enum for the type of execution environment.

    """
    LOCAL = enum.auto()
    LAMBDA = enum.auto()


class HistoricalDataType(enum.Enum):
    """Enum for the type of historical data being handled.

    """
    QUOTES = enum.auto()
    TRADES = enum.auto()


class AsyncWriteFileGzip(threading.Thread):
    """Gzips and writes a file on a separate thread.

    """
    def __init__(self, environment_type: EnvironmentType, data: bytes,
                 relative_path: str) -> None:
        """Initialize the environment, data, and relative path to write.

        Args:
            environment_type: Enum indicating how data should be written.
            data: Data to write.
            relative_path: Relative path to write. Does not support single or
                double dots.

        """
        threading.Thread.__init__(self)
        self._environment_type = environment_type
        self._data = data
        self._relative_path = relative_path

    def run(self) -> None:
        """Gzip file and write to either local filesystem or S3 bucket depending
        on environment.

        Raises:
            OSError: An error occurred when trying to write to the local
                filesystem.
            botocore.exceptions.ClientError: An error occurred when trying to
                write to an S3 bucket.

        """
        logger = logging.getLogger(__name__)
        if self._environment_type is EnvironmentType.LOCAL:
            try:
                logger.info('Writing local file | relative_path: %s',
                            self._relative_path)
                with gzip.open(self._relative_path, 'wb') as file_object:
                    file_object.write(self._data)
            except OSError as exception:
                logger.error('Local file write failed')
                raise exception
        elif self._environment_type is EnvironmentType.LAMBDA:
            # The first directory in the relative path is used as the s3 bucket
            # name when running in Lambda.
            s3_bucket = self._relative_path.split('/')[0]
            s3_key = '/'.join(self._relative_path.split('/')[1:])
            s3_client = boto3.client('s3')
            try:
                logger.info(
                    'Writing S3 object | %s',
                    's3_bucket: {}, s3_key: {}'.format(s3_bucket, s3_key))
                s3_client.put_object(Body=gzip.compress(self._data),
                                     Bucket=s3_bucket,
                                     Key=s3_key)
            except botocore.exceptions.ClientError as exception:
                logger.error('S3 object write failed')
                raise exception


def make_directory(relative_path: str) -> None:
    """Make a new directory if it doesn't exist, and also make any parent
    directories that don't exist.

    Args:n
        relative_path: Relative path to directory. Does not support single or
            double dots.

    Raises:
        OSError: An error occurred when trying to make a directory.

    """
    logger = logging.getLogger(__name__)

    directories = list(filter(None, relative_path.split('/')))
    for i, _ in enumerate(directories):
        local_path = '/'.join(directories[:(i + 1)])
        if not os.path.exists(local_path):
            try:
                logger.info('Making directory | local_path: %s', local_path)
                os.makedirs(local_path)
            except OSError as exception:
                logger.error('Make directory failed')
                raise exception


def fetch_csv_data(historical_data_type: HistoricalDataType, api_key: str,
                   response_limit: int, symbol: str, date: str) -> list:
    """Use the REST API to get historical data, querying as many times as needed
    to get all the data for a single symbol and date. Iteratively append to CSV
    for each response.

    Args:
        historical_data_type: Enum indicating which type of data to fetch.
        api_key: Polygon-issued API key.
        response_limit: Max records returned per API request.
        symbol: Ticker symbol.
        date: YYYY-MM-DD format.

    Returns:
        CSV-formatted string.

    Raises:
        SystemExit: An API call wasn't successful.

    """
    logger = logging.getLogger(__name__)
    client = polygon.RESTClient(api_key)
    min_timestamp = 0
    last_response = None

    # Initialize CSV with header.
    if historical_data_type is HistoricalDataType.QUOTES:
        csv_data = [
            'sequence_number,sip_timestamp,exchange_timestamp,'
            'bid_price,bid_size,bid_exchange,ask_price,ask_size,ask_exchange,'
            'conditions,indicators'
        ]
    elif historical_data_type is HistoricalDataType.TRADES:
        csv_data = [
            'sequence_number,sip_timestamp,exchange_timestamp,'
            'price,size,exchange,conditions'
        ]


    while True:
        # Fetch response from Polygon API, and exit on failure.
        logger.info(
            'Fetching responses | %s', 'historical_data_type: {}, ticker: {}'
            ', date: {}, timestamp: {}, limit: {}'.format(
                historical_data_type.name, symbol, date, min_timestamp,
                response_limit))
        if historical_data_type is HistoricalDataType.QUOTES:
            response = client.historic_n___bbo_quotes_v2(
                ticker=symbol,
                date=date,
                timestamp=min_timestamp,
                limit=response_limit)
        elif historical_data_type is HistoricalDataType.TRADES:
            response =  client.historic_trades_v2(
                ticker=symbol,
                date=date,
                timestamp=min_timestamp,
                limit=response_limit)
        if not response.success:
            logger.error('Fetch failed')
            sys.exit(1)

        logger.info('Fetch succeeded | %s',
                    'results_count: {}'.format(response.results_count))

        # Add row to CSV for each unique result.
        for i, result in enumerate(response.results):
            # Remove duplicate rows from the end of the previous result, i.e.
            # all rows which have the same SIP timestamp as the first row of
            # this result.
            if i == 0 and last_response is not None:
                j = -1
                while True:
                    last_result = last_response.results[j]
                    if last_result['t'] == result['t']:
                        duplicate_row = csv_data.pop()
                        logger.info('Removing duplicate row | %s',
                                    duplicate_row)
                        j -= 1
                    else:
                        break
            if historical_data_type is HistoricalDataType.QUOTES:
                csv_data.append('{},{},{},{},{},{},{},{},{},{},{}'.format(
                    result['q'], result['t'], result['y'], result['p'],
                    result['s'], result['x'], result['P'], result['S'],
                    result['X'], ' '.join(map(str, result.get('c', []))),
                    ' '.join(map(str, result.get('i', [])))))
            elif historical_data_type is HistoricalDataType.TRADES:
                csv_data.append('{},{},{},{},{},{},{}'.format(
                    result['q'], result['t'], result['y'], result['p'],
                    result['s'], result['x'],
                    ' '.join(map(str, result.get('c', [])))))

        # Loop until a response is received with fewer results than the maximum.
        if response.results_count < response_limit:
            break

        # Update state for next iteration.
        min_timestamp = response.results[-1]['t']
        last_response = response

    return '\n'.join(csv_data) + '\n'


def validate_timestamps(csv_data: str, time_zone: datetime.tzinfo,
                        max_time_start: datetime.time,
                        min_time_end: datetime.time,
                        max_time_delta: datetime.timedelta) -> None:
    """Perform a few sanity checks on the timestamps in CSV data.

    Args:
        csv_data: CSV-formatted string.
        time_zone: Time zone for converting Unix time.
        max_time_start: The latest time the data should begin.
        min_time_end: The earliest time the data should stop.
        max_time_delta: The longest time gap that is allowed.

    Raises:
        SystemExit: Validation failed.

    """
    logger = logging.getLogger(__name__)

    # Create dataframe from CSV timestamps.
    logger.info('Validating CSV timestamps')
    df = pd.read_csv(io.StringIO(csv_data),
                     usecols=['sip_timestamp'],
                     dtype={'sip_timestamp': 'int64'})

    # Populate helper columns.
    df['datetime'] = [
        datetime.datetime.fromtimestamp(t, time_zone)
        for t in df['sip_timestamp'] / 10.0**9
    ]
    df['time'] = [t.time() for t in df['datetime']]
    df['timedelta'] = (df['datetime'] - df['datetime'].shift()).fillna(
        pd.Timedelta(0))

    # if sum(df.duplicated()) > 0:
    #     logger.error('Validation failed for duplicates check')
    #     sys.exit(1)

    if df['time'].iloc[0] > max_time_start:
        logger.error('Validation failed for max_time_start')
        sys.exit(1)

    if df['time'].iloc[-1] < min_time_end:
        logger.error('Validation failed for min_time_end')
        sys.exit(1)

    # Check max time delta, only considering timestamps after max start and
    # before min end times.
    if max(df['timedelta'][(df['time'] >= max_time_start)
                           & (df['time'] <= min_time_end)]) > max_time_delta:
        logger.error('Validation failed for max_time_delta')
        sys.exit(1)


def main_local() -> None:
    """ Start execution when running locally.

    """
    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file',
                        metavar='FILE',
                        help='config YAML',
                        default='polygon_download_config.yaml')
    parser.add_argument('--secrets_file',
                        metavar='FILE',
                        help='secrets YAML',
                        default='polygon_download_secrets.yaml')
    args = parser.parse_args()

    # Load YAML files into dicts.
    with open(args.config_file, 'r') as config_file:
        config = yaml.safe_load(config_file.read())
    with open(args.secrets_file, 'r') as secrets_file:
        secrets = yaml.safe_load(secrets_file.read())

    main_common(EnvironmentType.LOCAL, config, secrets)


# TODO: Add type hint for context.
def main_lambda(event: dict, context) -> None:
    """ Start execution when running on AWS Lambda.

    """
    # Load config from Lambda event.
    config = event['polygon_download_config']

    # Load secrets from deployed YAML file.
    with open('polygon_download_secrets.yaml', 'r') as secrets_file:
        secrets = yaml.safe_load(secrets_file.read())

    main_common(EnvironmentType.LAMBDA, config, secrets)


def main_common(environment_type: EnvironmentType, config: dict,
                secrets: dict) -> None:
    """Start execution of common script logic.

    """
    # Initialize logger.
    logging.config.dictConfig(config['logging'])

    # Initialize date and time variables from config.
    time_zone = pytz.timezone(config['time_zone'])
    max_time_start = datetime.time(hour=config['max_start_hour'],
                                   minute=config['max_start_minute'])
    min_time_end = datetime.time(hour=config['min_end_hour'],
                                 minute=config['min_end_minute'])
    max_time_delta = datetime.timedelta(minutes=config['max_delta_minutes'])

    # Threads for writing files async.
    threads = []

    # Process each date and symbol in the config.
    for date in config['dates']:
        for symbol in config['symbols']:
            # Populate file prefix and make new directories as needed.
            file_prefix = '/'.join([config['download_location'], date, symbol
            ]) + '/'
            if environment_type is EnvironmentType.LOCAL:
                make_directory(file_prefix)

            # Fetch quotes CSV, validate, and write to file.
            if 'quotes_csv_filename' in config:
                quotes_csv_data = fetch_csv_data(HistoricalDataType.QUOTES,
                                                 secrets['api_key'],
                                                 config['response_limit'],
                                                 symbol, date)
                validate_timestamps(quotes_csv_data, time_zone, max_time_start,
                                    min_time_end, max_time_delta)
                threads.append(
                    AsyncWriteFileGzip(
                        environment_type, quotes_csv_data.encode(),
                        file_prefix + config['quotes_csv_filename']))
                threads[-1].start()

            # Fetch trades CSV, validate, and write to file.
            if 'trades_csv_filename' in config:
                trades_csv_data = fetch_csv_data(HistoricalDataType.TRADES,
                                                 secrets['api_key'],
                                                 config['response_limit'],
                                                 symbol, date)
                validate_timestamps(trades_csv_data, time_zone, max_time_start,
                                    min_time_end, max_time_delta)
                threads.append(
                    AsyncWriteFileGzip(
                        environment_type, trades_csv_data.encode(),
                        file_prefix + config['trades_csv_filename']))
                threads[-1].start()

    # Wait for files to finish writing async.
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main_local()
