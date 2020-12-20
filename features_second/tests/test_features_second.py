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
    """Test that high_price_day is populated correctly.

    """
    assert output_df.at[7, 'high_price_day'] == 248.15


def test_output_df_low_price_day(output_df):
    """Test that low_price_day is populated correctly.

    """
    assert output_df.at[5, 'low_price_day'] == 247.95


def test_output_df_volatility_day(output_df):
    """Test that volatility_day is populated correctly.

    """
    assert output_df.at[14, 'volatility_day'] == 0.9199750243590508


def test_output_df_vwap_day(output_df):
    """Test that vwap_day is populated correctly.

    """
    assert output_df.at[14, 'vwap_day'] == 246.63756958317367


def test_output_df_volume_total_day(output_df):
    """Test that volume_total_day is populated correctly.

    """
    assert output_df.at[9, 'volume_total_day'] == 793936


def test_output_df_volume_aggressive_buy_day(output_df):
    """Test that volume_aggressive_buy_day is populated correctly.

    """
    assert output_df.at[6, 'volume_aggressive_buy_day'] == 14056


def test_output_df_volume_aggressive_sell_day(output_df):
    """Test that volume_aggressive_sell_day is populated correctly.

    """
    assert output_df.at[15, 'volume_aggressive_sell_day'] == 882118


def test_output_df_message_count_quote_day(output_df):
    """Test that message_count_quote_day is populated correctly.

    """
    assert output_df.at[9, 'message_count_quote_day'] == 2077


def test_output_df_message_count_trade_day(output_df):
    """Test that message_count_trade_day is populated correctly.

    """
    assert output_df.at[14, 'message_count_trade_day'] == 3389


def test_output_df_high_price_3(output_df):
    """Test that high_price_3 is populated correctly.

    """
    assert output_df.at[12, 'high_price_3'] == 246.53


def test_output_df_low_price_3(output_df):
    """Test that low_price_3 is populated correctly.

    """
    assert output_df.at[12, 'low_price_3'] == 246.32


def test_output_df_volatility_3(output_df):
    """Test that volatility_3 is populated correctly.

    """
    assert output_df.at[12, 'volatility_3'] == 0.040414518841398066


def test_output_df_moving_average_3(output_df):
    """Test that moving_average_3 is populated correctly.

    """
    assert output_df.at[12, 'moving_average_3'] == 246.4266666666667


def test_output_df_moving_average_weighted_3(output_df):
    """Test that moving_average_weighted_3 is populated correctly.

    """
    assert output_df.at[12, 'moving_average_weighted_3'] == 246.41499999999996


def test_output_df_bid_size_median_3(output_df):
    """Test that bid_size_median_3 is populated correctly.

    """
    assert output_df.at[12, 'bid_size_median_3'] == 300


def test_output_df_ask_size_median_3(output_df):
    """Test that ask_size_median_3 is populated correctly.

    """
    assert output_df.at[12, 'ask_size_median_3'] == 2200


def test_output_df_bid_ask_spread_median_3(output_df):
    """Test that bid_ask_spread_median_3 is populated correctly.

    """
    assert output_df.at[12, 'bid_ask_spread_median_3'] == 0.009999999999990905


def test_output_df_vwap_3(output_df):
    """Test that vwap_3 is populated correctly.

    """
    assert output_df.at[12, 'vwap_3'] == 246.41663960217127


def test_output_df_volume_total_3(output_df):
    """Test that volume_total_3 is populated correctly.

    """
    assert output_df.at[12, 'volume_total_3'] == 156883


def test_output_df_volume_aggressive_buy_3(output_df):
    """Test that volume_aggressive_buy_3 is populated correctly.

    """
    assert output_df.at[12, 'volume_aggressive_buy_3'] == 72146


def test_output_df_volume_aggressive_sell_3(output_df):
    """Test that volume_aggressive_sell_3 is populated correctly.

    """
    assert output_df.at[12, 'volume_aggressive_sell_3'] == 84102


def test_output_df_message_count_quote_3(output_df):
    """Test that message_count_quote_3 is populated correctly.

    """
    assert output_df.at[12, 'message_count_quote_3'] == 2433


def test_output_df_message_count_trade_3(output_df):
    """Test that message_count_trade_3 is populated correctly.

    """
    assert output_df.at[12, 'message_count_trade_3'] == 772
