#!/usr/bin/env python3
"""Joins columns of data together in CSV format.

"""
import concurrent.futures
import gzip
import json
import logging
import logging.config
import os
# from typing import List

# import numpy as np
import pandas as pd

import reup_utils


def main_lambda(event: dict, context) -> None:
    """Start execution when running on Lambda.

    Args:
        event: Lambda event provided by environment.
        context: Lambda context provided by environment. This is not used, and
            doesn't have a type hint as type is defined by Lambda.

    """
    # pylint: disable=unused-argument

    # Initialize logger.
    logging.config.dictConfig(event['logging'])
    logger = logging.getLogger(__name__)
    logger.info(json.dumps(event))

    data_frames = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=event['s3_max_workers']) as executor:
        futures = []
        for s3_input in event['s3_inputs']:
            futures.append(
                executor.submit(reup_utils.download_s3_object,
                                event['s3_bucket'],
                                s3_input['s3_key'],
                                thread_safe=True))

        for future in concurrent.futures.as_completed(futures):
            local_path = future.result()
            logger.info(local_path)
            with gzip.open(local_path, 'rb') as gzip_file:
                # TODO: Add dtype info to read_csv.
                data_frames.append(pd.read_csv(gzip_file))

            os.remove(local_path)

    for df in data_frames:
        print(df.head())

    # for i in range(len(data_frames)):
    #     data_frames[i] = data_frames[i].add_prefix(symbols[i] + '_')

    # print(pd.concat(data_frames, axis=1).to_string())

    #     time_series_df = pd.read_csv(gzip_file,
    #                                  dtype={
    #                                      'timestamp': 'float64',
    #                                      'bid_price': 'float64',
    #                                      'bid_size': 'int64',
    #                                      'ask_price': 'float64',
    #                                      'ask_size': 'int64',
    #                                      'last_trade_price': 'float64',
    #                                      'vwap': 'float64',
    #                                      'volume_price_dict': 'string',
    #                                      'volume_total': 'int64',
    #                                      'volume_aggressive_buy': 'int64',
    #                                      'volume_aggressive_sell': 'int64',
    #                                      'message_count_quote': 'int64',
    #                                      'message_count_trade': 'int64'
    #                                  })

    # output_df = get_output_df(time_series_df, event['time_windows'],
    #                           open_timestamp, close_timestamp, weekday)
    # reup_utils.upload_s3_object(
    #     event['s3_bucket'], event['s3_key_output'],
    #     gzip.compress(output_df.to_csv(index=False).encode()))
