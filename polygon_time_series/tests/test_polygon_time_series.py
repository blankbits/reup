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
    assert np.abs(1.0 - deltas.max()) <= np.finfo(np.float64).eps
    assert np.abs(1.0 - deltas.min()) <= np.finfo(np.float64).eps
