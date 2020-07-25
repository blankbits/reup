#!/usr/bin/env python3
"""Contains a main function which downloads historical quote and trade data from
Polygon, does some basic validation, and dumps both the raw API responses and
CSV formatted data to disk.

It defaults to using the config specified in polygon_downloader_config.yaml, but
this can be overridden via a command line arg.

Example:
    ./polygon_downloader.py --config_file custom_config.yaml

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

import pandas as pd
import polygon
import pytz
import yaml


class HistoricalDataType(enum.Enum):
    """Enum for the type of historical data being handled.

    """
    QUOTES = enum.auto()
    TRADES = enum.auto()


class AsyncWriteFileGzip(threading.Thread):
    """Writes and gzips a file on a separate thread.

    """
    def __init__(self, data: bytes, path: str) -> None:
        """Initialize the data and path to write.

        Args:
            data: Data to write.
            path: Path to write.

        """
        threading.Thread.__init__(self)
        self._data = data
        self._path = path

    def run(self) -> None:
        """Write and gzip file.

        Raises:
            SystemExit: An error occurred when trying to write the file.

        """
        logger = logging.getLogger(__name__)
        try:
            logger.info('Writing file | path: %s', self._path)
            with gzip.open(self._path, 'wb') as file_object:
                file_object.write(self._data)
        except OSError:
            logger.error('Write failed')
            sys.exit(1)


def make_directory(path: str) -> None:
    """Make a new directory if it doesn't exist, and also make any parent
    directories that don't exist.

    Args:
        path: Directory path.

    Raises:
        SystemExit: An error occurred when trying to make a directory.

    """
    logger = logging.getLogger(__name__)

    directories = list(filter(None, path.split('/')))
    for i, _ in enumerate(directories):
        local_path = '/'.join(directories[:(i + 1)])
        if not os.path.exists(local_path):
            try:
                logger.info('Making directory | path: %s', local_path)
                os.makedirs(local_path)
            except OSError:
                logger.error('Make directory failed')
                sys.exit(1)


def fetch_responses(historical_data_type: HistoricalDataType, api_key: str,
                    response_limit: int, symbol: str, date: str) -> list:
    """Use the REST API to get historical data, querying as many times as needed
    to get all the data for a single symbol and date.

    Args:
        historical_data_type: Enum indicating which type of data to fetch.
        api_key: Polygon-issued API key.
        response_limit: Max records returned per API request.
        symbol: Ticker symbol.
        date: YYYY-MM-DD format.

    Returns:
        List of the raw API responses.

    Raises:
        SystemExit: An API call wasn't successful.

    """
    logger = logging.getLogger(__name__)
    responses = []
    min_timestamp = 0
    client = polygon.RESTClient(api_key)

    while True:
        logger.info(
            'Fetching responses | %s', 'historical_data_type: {}, ticker: {}'
            ', date: {}, timestamp: {}, limit: {}'.format(
                historical_data_type.name, symbol, date, min_timestamp,
                response_limit))

        if historical_data_type is HistoricalDataType.QUOTES:
            responses.append(
                client.historic_n___bbo_quotes_v2(ticker=symbol,
                                                  date=date,
                                                  timestamp=min_timestamp,
                                                  limit=response_limit))
        elif historical_data_type is HistoricalDataType.TRADES:
            responses.append(
                client.historic_trades_v2(ticker=symbol,
                                          date=date,
                                          timestamp=min_timestamp,
                                          limit=response_limit))

        if not responses[-1].success:
            logger.error('Fetch failed')
            sys.exit(1)

        logger.info('Fetch succeeded | %s',
                    'results_count: {}'.format(responses[-1].results_count))
        if responses[-1].results_count < response_limit:
            break

        min_timestamp = responses[-1].results[-1]['t']

    return responses


def generate_csv(historical_data_type: HistoricalDataType,
                 responses: list) -> str:
    """Convert the raw API responses for historical data into CSV format,
    discarding extraneous data.

    Args:
        historical_data_type: Enum indicating the type of responses.
        responses: Raw API responses for a single symbol and date.

    Returns:
        CSV-formatted string.

    Raises:
        SystemExit: Unexpected lack of overlap between API responses.

    """
    logger = logging.getLogger(__name__)

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

    logger.info('Generating CSV | historical_data_type: %s',
                historical_data_type.name)
    for i, response in enumerate(responses):
        for j, result in enumerate(response.results):
            # Remove duplicate rows from the end of the previous result, i.e.
            # all rows which have the same SIP timestamp as the first row of
            # this result.
            if i > 0 and j == 0:
                k = -1
                while True:
                    last_result = responses[i - 1].results[k]
                    if last_result['t'] == result['t']:
                        duplicate_row = csv_data.pop()
                        logger.info('Removing duplicate row | %s',
                                    duplicate_row)
                        k -= 1
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


def load_config() -> dict:
    """ Load config from YAML file.

    """
    # Parse command line args and load config.
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file',
                        metavar='FILE',
                        help='config YAML',
                        default='polygon_downloader_config.yaml')
    args = parser.parse_args()
    with open(args.config_file, 'r') as config_file:
        config = yaml.safe_load(config_file.read())

    return config


def main():
    """Begin executing main logic of the script.

    """
    # Load config and setup logger.
    config = load_config()
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
            file_prefix = '/'.join([config['download_path'], date, symbol
                                    ]) + '/'
            make_directory(file_prefix)

            # Fetch raw quotes API responses if needed for writing files.
            if ('quotes_responses_filename' in config
                    or 'quotes_csv_filename' in config):
                quotes_responses = fetch_responses(HistoricalDataType.QUOTES,
                                                   config['api_key'],
                                                   config['response_limit'],
                                                   symbol, date)

            # Write raw quotes API responses to file.
            if 'quotes_responses_filename' in config:
                threads.append(
                    AsyncWriteFileGzip(
                        pickle.dumps(quotes_responses),
                        file_prefix + config['quotes_responses_filename']))
                threads[-1].start()

            # Generate quotes CSV from responses, validate, and write to file.
            if 'quotes_csv_filename' in config:
                quotes_csv_data = generate_csv(HistoricalDataType.QUOTES,
                                               quotes_responses)
                validate_timestamps(quotes_csv_data, time_zone, max_time_start,
                                    min_time_end, max_time_delta)
                threads.append(
                    AsyncWriteFileGzip(
                        quotes_csv_data.encode(),
                        file_prefix + config['quotes_csv_filename']))
                threads[-1].start()

            # Fetch raw trades API responses if needed for writing files.
            if ('trades_responses_filename' in config
                    or 'trades_csv_filename' in config):
                trades_responses = fetch_responses(HistoricalDataType.TRADES,
                                                   config['api_key'],
                                                   config['response_limit'],
                                                   symbol, date)

            # Write raw trades API responses to file.
            if 'trades_responses_filename' in config:
                threads.append(
                    AsyncWriteFileGzip(
                        pickle.dumps(trades_responses),
                        file_prefix + config['trades_responses_filename']))
                threads[-1].start()

            # Generate trades CSV from responses, validate, and write to file.
            if 'trades_csv_filename' in config:
                trades_csv_data = generate_csv(HistoricalDataType.TRADES,
                                               trades_responses)
                validate_timestamps(trades_csv_data, time_zone, max_time_start,
                                    min_time_end, max_time_delta)
                threads.append(
                    AsyncWriteFileGzip(
                        trades_csv_data.encode(),
                        file_prefix + config['trades_csv_filename']))
                threads[-1].start()

    # Wait for files to finish writing async.
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
