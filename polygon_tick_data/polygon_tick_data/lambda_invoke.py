#!/usr/bin/env python3
"""Contains LambdaInvokeSimple subclass to handle polygon tick data specifics.

"""
import json
from typing import Dict, List

import reup_utils


class LambdaInvoke(reup_utils.LambdaInvokeSimple):
    """LambdaInvokeSimple subclass to handle polygon tick data specifics.

    get_lambda_payload and get_pending_invocations implementations below
    override those methods in the base class.

    """
    def get_lambda_payload(self, date: str, symbol: str) -> bytes:
        """Build the Lambda payload for a function invocation.

        Args:
            date: Date for this invocation in YYYY-MM-DD format.
            symbol: Symbol for this invocation.

        Returns:
            JSON bytes to be used as Lambda payload.

        """
        self._lambda_event['dates'] = [date]
        self._lambda_event['symbols'] = [symbol]
        self._lambda_event['download_location'] = self._config[
            'download_location']
        return json.dumps(self._lambda_event).encode()

    def get_pending_invocations(
            self, date_symbol_dict: Dict[str,
                                         List[str]]) -> Dict[str, List[str]]:
        """Find pending Lambda invocations by checking whether output S3 objects
        exist for the given dates, symbols, and other parameters. Returns the
        dates and symbols which are missing S3 objects.

        Args:
            date_symbol_dict: Dict with date keys and symbol list values.

        Returns:
            Dict with date keys and symbol list values.

        """
        s3_bucket = self._config['download_location'].split('/')[0]
        s3_key_prefix = '/'.join(
            self._config['download_location'].split('/')[1:])

        pending_date_symbol_dict: Dict[str, List[str]] = {}
        for date in sorted(date_symbol_dict.keys()):
            s3_keys = set(
                reup_utils.get_s3_keys(s3_bucket,
                                       s3_key_prefix + '/' + date + '/'))
            for symbol in sorted(date_symbol_dict[date]):
                s3_key_quotes = '/'.join([
                    s3_key_prefix, date, symbol,
                    self._lambda_event['quotes_csv_filename']
                ])
                s3_key_trades = '/'.join([
                    s3_key_prefix, date, symbol,
                    self._lambda_event['trades_csv_filename']
                ])
                if (s3_key_quotes not in s3_keys
                        or s3_key_trades not in s3_keys):
                    if date in pending_date_symbol_dict:
                        if pending_date_symbol_dict[date][-1] != symbol:
                            pending_date_symbol_dict[date].append(symbol)
                    else:
                        pending_date_symbol_dict[date] = [symbol]

        return pending_date_symbol_dict
