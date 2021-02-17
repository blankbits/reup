#!/usr/bin/env python3
"""Run the entire reup pipeline.

"""
import argparse
import json
import logging
import logging.config
import sys
import time
from typing import Dict, List

import yaml

import reup_utils
import polygon_tick_data.lambda_invoke as ptd_lambda_invoke
import polygon_time_series.lambda_invoke as pts_lambda_invoke


def run_stage(lambda_invoke: reup_utils.LambdaInvokeSimple,
              date_symbol_dict: Dict[str, List[str]],
              max_retry_count: int,
              retry_seconds: int) -> None:
    logger = logging.getLogger(__name__)
    retry_count = 0
    while retry_count <= max_retry_count:
        pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
            date_symbol_dict)
        if len(pending_date_symbol_dict) == 0:
            return

        lambda_invoke.run(pending_date_symbol_dict)
        logger.info('Sleeping | %s', 'retry_count:{}, retry_seconds:{}'.format(
            retry_count, retry_seconds))
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
    parser.add_argument('--config_file', metavar='FILE', help='config YAML',
                        default='pipeline_config.yaml')
    args = parser.parse_args()

    # Load config YAML file.
    with open(args.config_file, 'r') as f:
        config = yaml.safe_load(f.read())

    # Initialize logger.
    logging.config.dictConfig(config['logging'])
    logger = logging.getLogger(__name__)

    # universe = reup_utils.Universe('reup', 'universes/russell-3000')
    # print(universe.get_symbol_df('2019-08-08').head())
    # print(universe.get_symbol_list('2019-08-08')[0:10])
    date_symbol_dict = {
        '2020-01-02': ['GOOG', 'SPY'],
        '2020-01-03': ['GOOG', 'SPY']
    }

    # logger.info('EAT')
    # blarg()
    # return
    
    # Run polygon tick data.
    logger.info('Starting pipeline stage for polygon_tick_data')
    lambda_invoke = ptd_lambda_invoke.LambdaInvoke(
        '../polygon_tick_data/polygon_tick_data/lambda_invoke_config.yaml',
        '../polygon_tick_data/polygon_tick_data/lambda_event.json')
    run_stage(lambda_invoke, date_symbol_dict, 3, 30)

    
    # # Run polygon tick data.
    # lambda_invoke = ptd_lambda_invoke.LambdaInvoke(
    #     '../polygon_tick_data/polygon_tick_data/lambda_invoke_config.yaml',
    #     '../polygon_tick_data/polygon_tick_data/lambda_event.json')
    # pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
    #     date_symbol_dict)
    # print(pending_date_symbol_dict)
    # if len(pending_date_symbol_dict) > 0:
    #     lambda_invoke.run(pending_date_symbol_dict)
    
    
    # # Run polygon time series.
    # lambda_invoke = pts_lambda_invoke.LambdaInvoke(
    #     '../polygon_time_series/polygon_time_series/lambda_invoke_config.yaml',
    #     '../polygon_time_series/polygon_time_series/lambda_event.json')
    # pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
    #     date_symbol_dict)
    # print(pending_date_symbol_dict)
    # if len(pending_date_symbol_dict) > 0:
    #     lambda_invoke.run(pending_date_symbol_dict)
    
    
    # # Run features second.
    # lambda_invoke = reup_utils.LambdaInvokeSimple(
    #     '../features_second/features_second/lambda_invoke_config.yaml',
    #     '../features_second/features_second/lambda_event.json')
    # pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
    #     date_symbol_dict)
    # print(pending_date_symbol_dict)
    # if len(pending_date_symbol_dict) > 0:
    #     lambda_invoke.run(pending_date_symbol_dict)
    
    
    # # Run features day.
    # lambda_invoke = reup_utils.LambdaInvokeSimple(
    #     '../features_day/features_day/lambda_invoke_config.yaml',
    #     '../features_day/features_day/lambda_event.json')
    # pending_date_symbol_dict = lambda_invoke.get_pending_invocations(
    #     date_symbol_dict)
    # print(pending_date_symbol_dict)
    # if len(pending_date_symbol_dict) > 0:
    #     lambda_invoke.run(pending_date_symbol_dict)
    

# If in top-level script environment, run main().
if __name__ == '__main__':
    main()
