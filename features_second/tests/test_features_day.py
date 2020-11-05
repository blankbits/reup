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
    """Create output data frame from test data.

    """
    return fs.get_output_df(time_series_df, '2020-04-01')


# def test_output_df_high_price(output_df):
#     """Test that high price is populated correctly.

#     """
#     assert output_df.at[0, 'high_price'] == 248.2
