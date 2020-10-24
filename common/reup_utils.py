#!/usr/bin/env python3
"""Common utility functions.

"""
import logging
from typing import Dict, List
import uuid

import boto3
import botocore


def download_s3_object(s3_bucket: str,
                       s3_key: str,
                       local_path: str = '') -> str:
    """Download an S3 object. If local path isn't specified, a unique local path
    compatible with Lambda will be generated.

    Args:
        s3_bucket: S3 bucket name for object to download.
        s3_key: S3 key for object to download.
        local_path: Optional local path.

    Returns:
        Local path for downloaded object.

    """
    logger = logging.getLogger(__name__)
    if not local_path:
        local_path = '/tmp/{}'.format(uuid.uuid4())

    logger.info('Downloading S3 object | %s',
                's3_bucket:{}, s3_key:{}'.format(s3_bucket, s3_key))
    try:
        s3_client = boto3.client('s3')
        s3_client.download_file(s3_bucket, s3_key, local_path)
    except botocore.exceptions.ClientError as exception:
        logger.error('S3 object download failed')
        raise exception

    return local_path


def get_s3_keys(s3_bucket: str, s3_prefix: str) -> List[str]:
    """Find all the S3 keys in a bucket with a given prefix.

    Args:
        s3_bucket: Name of S3 bucket.
        s3_prefix: Prefix of S3 keys used to filter results.

    Returns:
        List of S3 keys.

    """
    logger = logging.getLogger(__name__)
    s3_client = boto3.client('s3')
    s3_keys: List[str] = []
    continuation_token = ''
    while True:
        logger.info(
            'Fetching S3 object list | %s',
            's3_bucket:{}, s3_prefix:{}, continuation_token:{}'.format(
                s3_bucket, s3_prefix, continuation_token))
        if continuation_token == '':
            response = s3_client.list_objects_v2(Bucket=s3_bucket,
                                                 Prefix=s3_prefix)
        else:
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=s3_prefix,
                ContinuationToken=continuation_token)

        if response['KeyCount'] > 0:
            for item in response['Contents']:
                s3_keys.append(item['Key'])

        if response['IsTruncated']:
            continuation_token = response['NextContinuationToken']
        else:
            break

    return s3_keys


def get_date_symbol_dict(s3_keys: List[str],
                         s3_prefix: str) -> Dict[str, List[str]]:
    """Parse S3 keys to find the set of unique date and symbol pairs.

    Args:
        s3_keys: List of S3 keys.
        s3_prefix: Prefix preceding the date and symbol in S3 keys.

    Returns:
        Dict with date keys and symbol list values.

    """
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
