#!/usr/bin/env python3
"""Tests the generation of features that are static for the entire day.

"""
import pandas as pd
import pytest

import features_day.features_day as fd


@pytest.fixture(name='time_series_df', scope='module')
def fixture_time_series_df():
    """Load time series test data.

    """
    return pd.read_csv('tests/time-series.csv')


@pytest.fixture(name='output_df', scope='module')
def fixture_output_df(time_series_df):
    """Create output data frame from test data.

    """
    return fd.get_output_df(time_series_df, [1, 2], 1585747800.0, 1585747810.0,
                            2)


def test_output_df_high_price(output_df):
    """Test that high price is populated correctly.

    """
    assert output_df.at[0, 'high_price'] == 248.2


def test_output_df_low_price(output_df):
    """Test that low price is populated correctly.

    """
    assert output_df.at[0, 'low_price'] == 247.92


def test_output_df_vwap_day(output_df):
    """Test that vwap day is populated correctly.

    """
    assert output_df.at[0, 'vwap_day'] == 247.97639356156992


def test_output_df_volume_total_day(output_df):
    """Test that volume total day is populated correctly.

    """
    assert output_df.at[0, 'volume_total_day'] == 917755


def test_output_df_weekday(output_df):
    """Test that weekday is populated correctly.

    """
    assert output_df.at[0, 'weekday'] == 2
