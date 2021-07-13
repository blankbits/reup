#!/usr/bin/env python3
"""Joins columns of data together in CSV format.

"""
import concurrent.futures
import gzip
import json
import logging
import logging.config
import os
import sys
from typing import List

# import numpy as np
import pandas as pd

import reup_utils


def get_output_df(data_frames: List[pd.DataFrame],
                  column_prefixes: List[str],
                  index_column_name: str = '') -> pd.DataFrame:
    """Create output data frame by joining columns of input data frames.

    Args:
        data_frames: List of data frames to join.
        column_prefixes: List of prefixes to use for the column names of each
            data frame. Must be the same length as data_frames.
        index_column_name (optional): Name of column to join on. This column
            must exist in each data frame.

    Returns:
        Output data frame.

    Raises:
        SystemExit: Inputs failed a basic sanity check.

    """
    logger = logging.getLogger(__name__)

    data_frames_output: List[pd.DataFrame] = []
    for i, _ in enumerate(data_frames):
        if column_prefixes[i]:
            data_frames_output.append(
                data_frames[i].add_prefix(column_prefixes[i] + '_'))
        else:
            data_frames_output.append(data_frames[i].copy())

    if index_column_name:
        # If an index is specified, all data frames need to contain the index
        # column in order to do the join.
        for df in data_frames:
            if index_column_name not in df:
                logger.error('Unable to join data frames because the specified'
                             'index column doesn\'t exist for all')
                sys.exit(1)

        for i, _ in enumerate(data_frames):
            if column_prefixes[i]:
                data_frames_output[i].set_index(column_prefixes[i] + '_' +
                                                index_column_name,
                                                inplace=True)
                data_frames_output[i].index.rename(index_column_name,
                                                   inplace=True)
            else:
                data_frames_output[i].set_index(index_column_name,
                                                inplace=True)

        output_df = pd.concat(data_frames_output, axis=1, join='inner')
        output_df.reset_index(level=0, inplace=True)

    else:
        # If no index is specified, then all data frames need to have the same
        # row count so that columns can be concatenated without causing NaNs.
        for df in data_frames:
            if len(df) != len(data_frames[0]):
                logger.error('Unable to join data frames with different row '
                             'counts if no index is specified')
                sys.exit(1)

        output_df = pd.concat(data_frames_output, axis=1)

    return output_df


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

    data_frames: List[pd.DataFrame] = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=event['s3_max_workers']) as executor:
        futures: List[concurrent.futures.Future] = []
        for s3_input in event['s3_inputs']:
            futures.append(
                executor.submit(reup_utils.download_s3_object,
                                event['s3_bucket'],
                                s3_input['s3_key'],
                                thread_safe=True))

        for future in futures:
            local_path = future.result()
            logger.info(local_path)
            with gzip.open(local_path, 'rb') as gzip_file:
                # No dtype info is provided to read_csv. This relies on the
                # assumption that since the only values modified are column
                # names, the default behavior won't corrupt output.
                data_frames.append(pd.read_csv(gzip_file))

            os.remove(local_path)

    column_prefixes = [i['column_prefix'] for i in event['s3_inputs']]

    output_df = get_output_df(data_frames, column_prefixes)
    reup_utils.upload_s3_object(
        event['s3_bucket'], event['s3_key_output'],
        gzip.compress(output_df.to_csv(index=False).encode()))
