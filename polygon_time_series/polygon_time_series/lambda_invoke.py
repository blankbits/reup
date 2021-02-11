#!/usr/bin/env python3
"""Contains LambdaInvokeSimple subclass to handle polygon time series specifics.

"""
import json

import reup_utils


class LambdaInvoke(reup_utils.LambdaInvokeSimple):
    """LambdaInvokeSimple subclass to handle polygon time series specifics.

    get_lambda_payload implementation overrides the base class method.

    """
    def get_lambda_payload(self, date: str, symbol: str) -> bytes:
        """Build the Lambda payload for a function invocation.

        Args:
            date: Date for this invocation in YYYY-MM-DD format.
            symbol: Symbol for this invocation.

        Returns:
            JSON bytes to be used as Lambda payload.

        """
        self._lambda_event['s3_bucket'] = self._config['s3_bucket']
        self._lambda_event['s3_key_quotes'] = (
            self._config['s3_key_input_prefix'] + date + '/' + symbol + '/' +
            self._config['s3_key_quotes_suffix'])
        self._lambda_event['s3_key_trades'] = (
            self._config['s3_key_input_prefix'] + date + '/' + symbol + '/' +
            self._config['s3_key_trades_suffix'])
        self._lambda_event['s3_key_output'] = (
            self._config['s3_key_output_prefix'] + date + '/' + symbol + '/' +
            self._config['s3_key_output_suffix'])
        return json.dumps(self._lambda_event).encode()
