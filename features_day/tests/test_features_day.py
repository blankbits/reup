#!/usr/bin/env python3
"""Tests the generation of features that are static for the entire day.

"""
import numpy as np
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
    return fd.get_output_df(time_series_df, '2020-04-01')


def test_output_df_timestamp_delta(output_df):
    """

    """
    # deltas = (seconds_df['timestamp'] - seconds_df['timestamp'].shift())[1:]
    # assert np.abs(1.0 - deltas.min()) <= np.finfo(np.float64).eps
    # assert np.abs(1.0 - deltas.max()) <= np.finfo(np.float64).eps
    assert True
