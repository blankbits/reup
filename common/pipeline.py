#!/usr/bin/env python3
"""Run the entire reup pipeline.

"""
import argparse
import json
import logging
import logging.config
import sys
import time
from typing import Dict, List, Tuple

import yaml

import reup_utils
import polygon_tick_data.lambda_invoke as ptd_lambda_invoke
import polygon_time_series.lambda_invoke as pts_lambda_invoke


def run_stage(lambda_invoke: reup_utils.LambdaInvokeSimple,
              date_symbol_dict: Dict[str, List[str]], max_retry_count: int,
              retry_seconds: int) -> None:
    """Run a single pipeline stage for all dates and symbols specified. If the
    initial run doesn't fully complete, and there are still pending dates and
    symbols, retry up to a max number of retries and exit upon failure.

    Args:
        lambda_invoke: Object representing the specific Lambda function to
            invoke and related behavior.
        date_symbol_dict: Dict with date keys and symbol list values.
        max_retry_count: Maximum number of times to retry incomplete stage.
        retry_seconds: Number of seconds to sleep before each retry.

    """
    logger = logging.getLogger(__name__)
    retry_count = 0
    while retry_count <= max_retry_count:
        pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
            date_symbol_dict)
        if len(pending_date_symbol_dict) == 0:
            return

        lambda_invoke.run(pending_date_symbol_dict)
        logger.info(
            'Sleeping | %s',
            'retry_count:{}, retry_seconds:{}'.format(retry_count,
                                                      retry_seconds))
        time.sleep(retry_seconds)
        retry_count += 1

    pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
        date_symbol_dict)
    if len(pending_date_symbol_dict) > 0:
        logger.error('Pipeline stage failed to complete')
        logger.error(json.dumps(pending_date_symbol_dict))
        sys.exit(1)


def main():
    """Begin executing main logic of the script.

    """
    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_file',
                        metavar='FILE',
                        help='config YAML',
                        default='pipeline_config.yaml')
    args = parser.parse_args()

    # Load config YAML file.
    with open(args.config_file, 'r') as f:
        config = yaml.safe_load(f.read())

    # Initialize logger.
    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger(__name__)

    # Initialize symbol universes. Do this outside the loop below to prevent
    # unnecessary S3 round trips.
    universes: Dict[reup_utils.Universe] = {}
    for s3_universe_prefix in config['s3_universe_prefixes']:
        universes[s3_universe_prefix] = reup_utils.Universe(
            config['s3_bucket'], s3_universe_prefix)

    # Initialize LambdaInvoke objects for each pipeline stage and add to list in
    # the order that the pipeline stages will run.
    lambda_invokers: List[Tuple[str, reup_utils.LambdaInvokeSimple]] = []
    lambda_invokers.append(
        ('polygon_tick_data',
         ptd_lambda_invoke.LambdaInvoke(
             config['polygon_tick_data']['lambda_invoke_config'],
             config['polygon_tick_data']['lambda_event'])))
    lambda_invokers.append(
        ('polygon_time_series',
         pts_lambda_invoke.LambdaInvoke(
             config['polygon_time_series']['lambda_invoke_config'],
             config['polygon_time_series']['lambda_event'])))
    lambda_invokers.append(
        ('features_second',
         reup_utils.LambdaInvokeSimple(
             config['features_second']['lambda_invoke_config'],
             config['features_second']['lambda_event'])))
    lambda_invokers.append(('features_day',
                            reup_utils.LambdaInvokeSimple(
                                config['features_day']['lambda_invoke_config'],
                                config['features_day']['lambda_event'])))

    # Loop thru dates and run full pipeline for each date.
    for date in config['dates']:
        # Get list of symbols from all universes.
        symbol_list: List[str] = []
        for name, universe in universes.items():
            logger.info('Getting universe symbols | %s',
                        'date:{}, name:{}'.format(date, name))
            symbol_list.extend(universe.get_symbol_list(date))

        # Remove duplicate symbols.
        symbol_list = sorted(list(set(symbol_list)))

        # Run all pipeline stages in order.
        for name, lambda_invoke in lambda_invokers:
            logger.info('Starting pipeline stage | %s',
                        'date:{}, name:{}'.format(date, name))
            run_stage(lambda_invoke, {date: symbol_list},
                      config[name]['max_retry_count'],
                      config[name]['retry_seconds'])


# If in top-level script environment, run main().
if __name__ == '__main__':
    main()
