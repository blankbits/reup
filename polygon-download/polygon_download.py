#!/usr/bin/env python3
"""Downloads historical quote and trade data from Polygon in CSV format. This
can be run either on a local machine or on Lambda.

Behavior is determined by config passed either as a YAML file or as a Lambda
event, and by a secrets YAML file containing the Polygon API key.

Example:
    ./polygon_download.py --config_file polygon_download_config.yaml \
        --secrets_file polygon_download_secrets.yaml

"""
import argparse
import enum
import gzip
import logging
import logging.config
import os
import sys
import threading

import boto3
import botocore
import polygon
import yaml


class EnvironmentType(enum.Enum):
    """Enum for the type of execution environment.

    """
    LOCAL = enum.auto()
    LAMBDA = enum.auto()


class HistoricalDataType(enum.Enum):
    """Enum for the type of historical data.

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
            environment_type: Enum for the type of execution environment.
            data: Data to write.
            relative_path: Relative path to write. Does not support single or
                double dots.

        """
        threading.Thread.__init__(self)
        self._environment_type = environment_type
        self._data = data
        self._relative_path = relative_path

    def run(self) -> None:
        """Gzip and write file to either local filesystem or S3 bucket depending
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
            # The first directory in the relative path is used as the S3 bucket
            # name when running on Lambda.
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

    Args:
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


def get_csv_header(historical_data_type: HistoricalDataType) -> str:
    """Generate a CSV header.

    Args:
        historical_data_type: Enum for the type of historical data.

    Returns:
        CSV-formatted string.

    """
    if historical_data_type is HistoricalDataType.QUOTES:
        csv_header = ('sequence_number,sip_timestamp,exchange_timestamp,'
                      'bid_price,bid_size,bid_exchange,'
                      'ask_price,ask_size,ask_exchange,'
                      'conditions,indicators')
    elif historical_data_type is HistoricalDataType.TRADES:
        csv_header = ('sequence_number,sip_timestamp,exchange_timestamp,'
                      'price,size,exchange,conditions')

    return csv_header


def append_csv_rows(historical_data_type: HistoricalDataType,
                    csv_strings: list, results, last_results) -> None:
    """Add a CSV row for each unique result, and remove duplicate rows as needed
    from the previous result set. This function modifies the csv_strings object.

    Args:
        historical_data_type: Enum for the type of historical data.
        csv_strings: List of CSV rows to append.
        results: List of raw API results. Doesn't have a type hint as type
            is defined by Polygon.
        last_results: List of raw API results from the previous response.
            Doesn't have a type hint as type is defined by Polygon.

    """
    logger = logging.getLogger(__name__)

    for i, result in enumerate(results):
        # Remove duplicate rows from the end of the last result set, i.e.
        # all rows which have the same SIP timestamp as the first row of
        # this result set.
        if i == 0 and last_results is not None:
            j = -1
            while True:
                trailing_result = last_results[j]
                if trailing_result['t'] == result['t']:
                    duplicate_row = csv_strings.pop()
                    logger.info('Removing duplicate row | %s', duplicate_row)
                    j -= 1
                else:
                    break

        if historical_data_type is HistoricalDataType.QUOTES:
            csv_strings.append('{},{},{},{},{},{},{},{},{},{},{}'.format(
                result['q'], result['t'], result['y'], result['p'],
                result['s'], result['x'], result['P'], result['S'],
                result['X'], ' '.join(map(str, result.get('c', []))),
                ' '.join(map(str, result.get('i', [])))))
        elif historical_data_type is HistoricalDataType.TRADES:
            csv_strings.append('{},{},{},{},{},{},{}'.format(
                result['q'], result['t'], result['y'], result['p'],
                result['s'], result['x'],
                ' '.join(map(str, result.get('c', [])))))


def fetch_csv_data(historical_data_type: HistoricalDataType, api_key: str,
                   response_limit: int, symbol: str, date: str) -> str:
    """Use the Polygon API to get historical data, querying as many times as
    needed to get all the data for a single symbol and date. Iteratively append
    to CSV for each response.

    Args:
        historical_data_type: Enum for the type of historical data.
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
    last_results = None

    # Initialize CSV with header.
    csv_strings = [get_csv_header(historical_data_type)]

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
            response = client.historic_trades_v2(ticker=symbol,
                                                 date=date,
                                                 timestamp=min_timestamp,
                                                 limit=response_limit)
        if not response.success:
            logger.error('Fetch failed')
            sys.exit(1)

        logger.info('Fetch succeeded | %s',
                    'results_count: {}'.format(response.results_count))

        # Add rows to CSV data for the current response.
        append_csv_rows(historical_data_type, csv_strings, response.results,
                        last_results)

        # Loop until a response is received with fewer results than the max.
        if response.results_count < response_limit:
            break

        # Update state for next iteration.
        min_timestamp = response.results[-1]['t']
        last_results = response.results

    return '\n'.join(csv_strings) + '\n'


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


def main_lambda(event: dict, context) -> None:
    """ Start execution when running on Lambda.

    Args:
        event: Lambda event provided by environment.
        context: Lambda context provided by environment. This is not used, and
            doesn't have a type hint as type is defined by Lambda.

    """
    # pylint: disable=unused-argument

    # Load config from Lambda event.
    config = event['polygon_download']

    # Load secrets from deployed YAML file.
    with open('polygon_download_secrets.yaml', 'r') as secrets_file:
        secrets = yaml.safe_load(secrets_file.read())

    main_common(EnvironmentType.LAMBDA, config, secrets)


def main_common(environment_type: EnvironmentType, config: dict,
                secrets: dict) -> None:
    """Start execution of common script logic.

    Args:
        environment_type: Enum for the type of execution environment.
        config: Config determining what data to fetch, where to write it,
            logging format, and other behavior.
        secrets: Contains Polygon API key.

    """
    # Initialize logger.
    logging.config.dictConfig(config['logging'])

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
