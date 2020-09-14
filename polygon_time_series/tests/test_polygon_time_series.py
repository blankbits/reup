#!/usr/bin/env python3
"""Tests the generation of time series data from tick data to ensure the logic
for sampling and aggregating is working as intended.

"""
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


def test_stub(seconds_df):
    """Test that test fixtures have been successfully loaded.

    """
    print(seconds_df.head())
    assert False
