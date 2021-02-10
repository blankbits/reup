#!/usr/bin/env python3
"""Invokes the Lambda function async using a template event loaded from a JSON
file.

The 'symbols', 'dates', and 'download_location' fields in the event are
overwritten so that an arbitrary number of Lambda function invocations can be
triggered at the same time to download data for different symbols and dates.

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

    # Load config YAML file and Lambda event JSON file. Overwrite JSON download
    # location with YAML value.
    with open(args.config_file, 'r') as f:
        config = yaml.safe_load(f.read())
    with open(args.lambda_event_file, 'r') as f:
        json_dict = json.load(f)
    json_dict['download_location'] = config['download_location']

    # Initialize logger.
    with open(config['logging_config'], 'r') as f:
        logging.config.dictConfig(yaml.safe_load(f.read()))
    logger = logging.getLogger(__name__)

    # Process each date and symbol in the config.
    client = boto3.client('lambda')
    event_count = 0
    for date in config['dates']:
        for symbol in config['symbols']:
            json_dict['dates'] = [date]
            json_dict['symbols'] = [symbol]

            # Invoke Lambda function async.
            logger.info('Invoking Lambda function async | %s',
                        'date:{}, symbol:{}'.format(date, symbol))
            response = client.invoke(
                FunctionName='polygon_tick_data',
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
