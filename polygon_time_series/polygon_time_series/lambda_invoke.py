#!/usr/bin/env python3
"""Invokes the Lambda function async using a template event loaded from a JSON
file.

The 's3_bucket', 's3_key_quotes', 's3_key_trades', and 's3_key_output' fields in
the event are overwritten so that an arbitrary number of Lambda function
invocations can be triggered at the same time to generate time series data for
different symbols and dates.

Example:
    ./lambda_invoke.py --config_file lambda_invoke_config.yaml \
        --lambda_event_file lambda_event.json

"""
import argparse
import json
import logging
import logging.config
import sys
import time

import boto3
import yaml

import reup_utils


def main() -> None:
    """Start execution.

    """
    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file',
                        metavar='FILE',
                        help='config YAML',
                        default='lambda_invoke_config.yaml')
    parser.add_argument('--lambda_event_file',
                        metavar='FILE',
                        help='Lambda event JSON',
                        default='lambda_event.json')
    args = parser.parse_args()

    # Load config YAML file and Lambda event JSON file.
    with open(args.config_file, 'r') as config_file:
        config = yaml.safe_load(config_file.read())
    with open(args.lambda_event_file, 'r') as json_file:
        json_dict = json.load(json_file)

    # Initialize logger.
    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger(__name__)

    # Get the set of date and symbol pairs found in the input S3 keys.
    s3_keys = reup_utils.get_s3_keys(config['s3_bucket'],
                                     config['s3_key_input_prefix'])
    date_symbol_dict = reup_utils.get_date_symbol_dict(
        s3_keys, config['s3_key_input_prefix'])

    # Process each date and symbol pair.
    client = boto3.client('lambda')
    event_count = 0
    for date in date_symbol_dict:
        for symbol in date_symbol_dict[date]:
            json_dict['polygon_time_series']['s3_bucket'] = config['s3_bucket']
            json_dict['polygon_time_series']['s3_key_quotes'] = (
                config['s3_key_input_prefix'] + date + '/' + symbol + '/' +
                config['s3_key_quotes_suffix'])
            json_dict['polygon_time_series']['s3_key_trades'] = (
                config['s3_key_input_prefix'] + date + '/' + symbol + '/' +
                config['s3_key_trades_suffix'])
            json_dict['polygon_time_series']['s3_key_output'] = (
                config['s3_key_output_prefix'] + date + '/' + symbol + '/' +
                config['s3_key_output_suffix'])

            # Check whether S3 output already exists.
            if len(
                    reup_utils.get_s3_keys(
                        config['s3_bucket'], json_dict['polygon_time_series']
                        ['s3_key_output'])) > 0:
                logger.info(
                    'Skipping Lambda invocation | %s',
                    's3_key_output:{}'.format(
                        json_dict['polygon_time_series']['s3_key_output']))
                continue

            # Invoke Lambda function async.
            logger.info('Invoking Lambda function async | %s',
                        'date:{}, symbol:{}'.format(date, symbol))
            response = client.invoke(
                FunctionName='polygon_time_series',
                InvocationType='Event',
                # LogType='',
                # ClientContext='',
                Payload=json.dumps(json_dict).encode(),
                # Qualifier=''
            )

            # Exit if invoke is unsuccessful.
            if response['ResponseMetadata']['HTTPStatusCode'] != 202:
                logger.error('Lambda invoke failed')
                logger.error(json.dumps(response))
                sys.exit(1)

            # Ensure the max number of concurrent events isn't exceeded.
            event_count += 1
            if event_count == config['max_event_count']:
                logger.info(
                    'Sleeping | %s', 'event_count:{}, sleep_seconds:{}'.format(
                        event_count, config['sleep_seconds']))
                time.sleep(config['sleep_seconds'])
                event_count = 0


if __name__ == '__main__':
    main()
