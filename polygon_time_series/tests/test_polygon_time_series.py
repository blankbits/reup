#!/usr/bin/env python3
"""Tests the generation of time series data from tick data to ensure the logic
for sampling and aggregating is working as intended.

"""
import numpy as np
import pandas as pd
import pytest

import polygon_time_series.polygon_time_series as pts


@pytest.fixture(name='quotes_df', scope='module')
def fixture_quotes_df():
    """Load quotes test data.

    """
    return pd.read_csv('tests/quotes.csv')


@pytest.fixture(name='trades_df', scope='module')
def fixture_trades_df():
    """Load trades test data.

    """
    return pd.read_csv('tests/trades.csv')


@pytest.fixture(name='seconds_df', scope='module')
def fixture_seconds_df(quotes_df, trades_df):
    """Create time series data frame from quotes and trades test data.

    """
    return pts.get_seconds_df(quotes_df, trades_df)


def test_seconds_df_timestamp_delta(seconds_df):
    """Test that min and max delta between timestamps is one second.

    """
    deltas = (seconds_df['timestamp'] - seconds_df['timestamp'].shift())[1:]
    assert np.abs(1.0 - deltas.min()) <= np.finfo(np.float64).eps
    assert np.abs(1.0 - deltas.max()) <= np.finfo(np.float64).eps


def test_seconds_df_timestamp_span(seconds_df, quotes_df):
    """Test that start and end timestamps match tick data.

    """
    start_time = np.ceil(quotes_df.at[0, 'sip_timestamp'] / 10.0**9)
    end_time = np.ceil(quotes_df.at[len(quotes_df) - 1, 'sip_timestamp'] /
                       10.0**9)
    assert (np.abs(start_time - seconds_df.at[0, 'timestamp']) <= np.finfo(
        np.float64).eps)
    assert (np.abs(end_time - seconds_df.at[len(seconds_df) - 1, 'timestamp'])
            <= np.finfo(np.float64).eps)


def test_seconds_df_empty_second(seconds_df):
    """Test that fields are populated correctly after a second of no tick data.

    """
    # This hardcoded value represents a second of data that was deleted from the
    # test tick data.
    start_timestamp = 1577977210.0
    first_row = seconds_df.loc[seconds_df['timestamp'] ==
                               start_timestamp].copy().reset_index()
    second_row = seconds_df.loc[seconds_df['timestamp'] == start_timestamp +
                                1.0].copy().reset_index()

    # Check that inside market and last trade price remain the same.
    assert second_row.at[0, 'bid_price'] == first_row.at[0, 'bid_price']
    assert second_row.at[0, 'bid_size'] == first_row.at[0, 'bid_size']
    assert second_row.at[0, 'ask_price'] == first_row.at[0, 'ask_price']
    assert second_row.at[0, 'ask_size'] == first_row.at[0, 'ask_size']
    assert second_row.at[0, 'last_trade_price'] == first_row.at[
        0, 'last_trade_price']

    # Check that volume aggregations are empty or zero.
    assert np.isnan(second_row.at[0, 'vwap'])
    assert np.isnan(second_row.at[0, 'volume_price_dict'])
    assert second_row.at[0, 'volume_total'] == 0
    assert second_row.at[0, 'volume_aggressive_buy'] == 0
    assert second_row.at[0, 'volume_aggressive_sell'] == 0

    # Check that message counts are zero.
    assert second_row.at[0, 'message_count_quote'] == 0
    assert second_row.at[0, 'message_count_trade'] == 0
