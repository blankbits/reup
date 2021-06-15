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
    return pd.read_csv('tests/time-series.csv',
                       dtype={
                           'timestamp': 'float64',
                           'bid_price': 'float64',
                           'bid_size': 'int64',
                           'ask_price': 'float64',
                           'ask_size': 'int64',
                           'last_trade_price': 'float64',
                           'vwap': 'float64',
                           'volume_price_dict': 'string',
                           'volume_total': 'int64',
                           'volume_aggressive_buy': 'int64',
                           'volume_aggressive_sell': 'int64',
                           'message_count_quote': 'int64',
                           'message_count_trade': 'int64'
                       })


@pytest.fixture(name='output_df', scope='module')
def fixture_output_df(time_series_df):
    """Create output data frame from test data.

    """
    return fd.get_output_df(time_series_df, [1, 2], 1585747801.0, 1585747810.0,
                            2)


def test_output_df_open_timestamp(output_df):
    """Test that open timestamp is populated correctly.

    """
    assert output_df.at[0, 'open_timestamp'] == 1585747801.0


def test_output_df_close_timestamp(output_df):
    """Test that close timestamp is populated correctly.

    """
    assert output_df.at[0, 'close_timestamp'] == 1585747810.0


def test_output_df_weekday(output_df):
    """Test that weekday is populated correctly.

    """
    assert output_df.at[0, 'weekday'] == 2


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


def test_output_df_bid_price_open_1(output_df):
    """Test that bid price open is populated correctly for window length 1.

    """
    assert output_df.at[0, 'bid_price_open_1'] == 247.98


def test_output_df_bid_size_open_1(output_df):
    """Test that bid size open is populated correctly for window length 1.

    """
    assert output_df.at[0, 'bid_size_open_1'] == 4700


def test_output_df_ask_price_open_1(output_df):
    """Test that ask price open is populated correctly for window length 1.

    """
    assert output_df.at[0, 'ask_price_open_1'] == 247.99


def test_output_df_ask_size_open_1(output_df):
    """Test that ask size open is populated correctly for window length 1.

    """
    assert output_df.at[0, 'ask_size_open_1'] == 500


def test_output_df_last_trade_price_open_1(output_df):
    """Test that last trade price open is populated correctly for window length
    1.

    """
    assert output_df.at[0, 'last_trade_price_open_1'] == 247.99


def test_output_df_vwap_open_1(output_df):
    """Test that vwap open is populated correctly for window length 1.

    """
    assert output_df.at[0, 'vwap_open_1'] == 247.95137560000003


def test_output_df_volume_total_open_1(output_df):
    """Test that volume total open is populated correctly for window length 1.

    """
    assert output_df.at[0, 'volume_total_open_1'] == 632974


def test_output_df_bid_price_close_1(output_df):
    """Test that bid price close is populated correctly for window length 1.

    """
    assert output_df.at[0, 'bid_price_close_1'] == 247.96


def test_output_df_bid_size_close_1(output_df):
    """Test that bid size close is populated correctly for window length 1.

    """
    assert output_df.at[0, 'bid_size_close_1'] == 200


def test_output_df_ask_price_close_1(output_df):
    """Test that ask price close is populated correctly for window length 1.

    """
    assert output_df.at[0, 'ask_price_close_1'] == 248.0


def test_output_df_ask_size_close_1(output_df):
    """Test that ask size close is populated correctly for window length 1.

    """
    assert output_df.at[0, 'ask_size_close_1'] == 600


def test_output_df_last_trade_price_close_1(output_df):
    """Test that last trade price close is populated correctly for window length
    1.

    """
    assert output_df.at[0, 'last_trade_price_close_1'] == 248.0


def test_output_df_vwap_close_1(output_df):
    """Test that vwap close is populated correctly for window length 1.

    """
    assert output_df.at[0, 'vwap_close_1'] == 247.9779063


def test_output_df_volume_total_close_1(output_df):
    """Test that volume total close is populated correctly for window length 1.

    """
    assert output_df.at[0, 'volume_total_close_1'] == 23399


def test_output_df_bid_price_open_2(output_df):
    """Test that bid price open is populated correctly for window length 2.

    """
    assert output_df.at[0, 'bid_price_open_2'] == 248.03


def test_output_df_bid_size_open_2(output_df):
    """Test that bid size open is populated correctly for window length 2.

    """
    assert output_df.at[0, 'bid_size_open_2'] == 900


def test_output_df_ask_price_open_2(output_df):
    """Test that ask price open is populated correctly for window length 2.

    """
    assert output_df.at[0, 'ask_price_open_2'] == 248.06


def test_output_df_ask_size_open_2(output_df):
    """Test that ask size open is populated correctly for window length 2.

    """
    assert output_df.at[0, 'ask_size_open_2'] == 2000


def test_output_df_last_trade_price_open_2(output_df):
    """Test that last trade price open is populated correctly for window length
    2.

    """
    assert output_df.at[0, 'last_trade_price_open_2'] == 248.03


def test_output_df_vwap_open_2(output_df):
    """Test that vwap open is populated correctly for window length >1.

    """
    assert output_df.at[0, 'vwap_open_2'] == 247.95632789863333


def test_output_df_volume_total_open_2(output_df):
    """Test that volume total open is populated correctly for window length >1.

    """
    assert output_df.at[0, 'volume_total_open_2'] == 697443


def test_output_df_bid_price_close_2(output_df):
    """Test that bid price close is populated correctly for window length 2.

    """
    assert output_df.at[0, 'bid_price_close_2'] == 247.97


def test_output_df_bid_size_close_2(output_df):
    """Test that bid size close is populated correctly for window length 2.

    """
    assert output_df.at[0, 'bid_size_close_2'] == 100


def test_output_df_ask_price_close_2(output_df):
    """Test that ask price close is populated correctly for window length 2.

    """
    assert output_df.at[0, 'ask_price_close_2'] == 247.98


def test_output_df_ask_size_close_2(output_df):
    """Test that ask size close is populated correctly for window length 2.

    """
    assert output_df.at[0, 'ask_size_close_2'] == 400


def test_output_df_last_trade_price_close_2(output_df):
    """Test that last trade price close is populated correctly for window length
    2.

    """
    assert output_df.at[0, 'last_trade_price_close_2'] == 247.9982


def test_output_df_vwap_close_2(output_df):
    """Test that vwap close is populated correctly for window length >1.

    """
    assert output_df.at[0, 'vwap_close_2'] == 247.96845473834776


def test_output_df_volume_total_close_2(output_df):
    """Test that volume total close is populated correctly for window length >1.

    """
    assert output_df.at[0, 'volume_total_close_2'] == 52801
