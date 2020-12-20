#!/usr/bin/env python3
"""Tests the generation of features from time series data for a single symbol on
a single day.

"""
import pandas as pd
import pytest

import features_second.features_second as fs


@pytest.fixture(name='time_series_df', scope='module')
def fixture_time_series_df():
    """Load time series test data.

    """
    return pd.read_csv('tests/time-series.csv')


@pytest.fixture(name='output_df', scope='module')
def fixture_output_df(time_series_df):
    """Create output data frame from test data with single length 3 time window.

    """
    return fs.get_output_df(time_series_df, [3])


def test_output_df_high_price_day(output_df):
    """Test that high price day is populated correctly.

    """
    assert output_df.at[7, 'high_price_day'] == 248.15


def test_output_df_low_price_day(output_df):
    """Test that low price day is populated correctly.

    """
    assert output_df.at[5, 'low_price_day'] == 247.95


def test_output_df_volatility_day(output_df):
    """Test that volatility day is populated correctly.

    """
    assert output_df.at[14, 'volatility_day'] == 0.9199750243590508


def test_output_df_vwap_day(output_df):
    """Test that vwap day is populated correctly.

    """
    assert output_df.at[14, 'vwap_day'] == 246.63756958317367


def test_output_df_volume_total_day(output_df):
    """Test that volume total day is populated correctly.

    """
    assert output_df.at[9, 'volume_total_day'] == 793936


def test_output_df_volume_aggressive_buy_day(output_df):
    """Test that volume aggressive buy day is populated correctly.

    """
    assert output_df.at[6, 'volume_aggressive_buy_day'] == 14056


def test_output_df_volume_aggressive_sell_day(output_df):
    """Test that volume aggressive sell day is populated correctly.

    """
    assert output_df.at[15, 'volume_aggressive_sell_day'] == 882118


def test_output_df_message_count_quote_day(output_df):
    """Test that message count quote day is populated correctly.

    """
    assert output_df.at[9, 'message_count_quote_day'] == 2077


def test_output_df_message_count_trade_day(output_df):
    """Test that message count trade day is populated correctly.

    """
    assert output_df.at[14, 'message_count_trade_day'] == 3389


# high_price_3	low_price_3	volatility_3	moving_average_3	moving_average_weighted_3	bid_size_median_3	ask_size_median_3	bid_ask_spread_median_3	vwap_3	volume_total_3	volume_aggressive_buy_3	volume_aggressive_sell_3	message_count_quote_3	message_count_trade_3
