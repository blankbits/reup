#!/usr/bin/env python3
import argparse
import json
import logging
import logging.config
import sys
import time
from typing import Dict, List

import boto3
import botocore
import yaml


def get_s3_keys(s3_bucket: str, s3_prefix: str) -> List[str]:
    logger = logging.getLogger(__name__)
    s3_client = boto3.client('s3')
    s3_keys: List[str] = []
    continuation_token = ''
    while True:
        logger.info('Fetching S3 object list | %s',
                    's3_bucket:{}, s3_prefix:{}, continuation_token:{}'.format(
                        s3_bucket, s3_prefix, continuation_token))
        if continuation_token == '':
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=s3_prefix
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=s3_prefix,
                ContinuationToken=continuation_token
            )

        if response['KeyCount'] > 0:
            for item in response['Contents']:
                s3_keys.append(item['Key'])

        if response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
        else:
            break

    return s3_keys


def get_date_symbol_dict(
        s3_keys: List[str], s3_prefix: str) -> Dict[str, List[str]]:
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
    s3_keys = get_s3_keys(config['s3_bucket'], config['s3_key_input_prefix'])
    date_symbol_dict = get_date_symbol_dict(
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
            if len(get_s3_keys(config['s3_bucket'],
                    json_dict['polygon_time_series']['s3_key_output'])) > 0:
                logger.info('Skipping Lambda invocation | %s',
                            's3_key_output:{}'.format(json_dict[
                                'polygon_time_series']['s3_key_output']))
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
