#!/usr/bin/env python3
"""Tests different scenarios for joining columns.

"""

import numpy as np
import pandas as pd
import pytest

import join_columns.join_columns as jc


@pytest.fixture(name='data_frame_0', scope='module')
def fixture_data_frame_0():
    """Load test data.

    """
    return pd.read_csv('tests/data-frame-0.csv')


@pytest.fixture(name='data_frame_1', scope='module')
def fixture_data_frame_1():
    """Load test data.

    """
    return pd.read_csv('tests/data-frame-1.csv')


@pytest.fixture(name='data_frame_2', scope='module')
def fixture_data_frame_2():
    """Load test data.

    """
    return pd.read_csv('tests/data-frame-2.csv')


@pytest.fixture(name='output_df_index', scope='module')
def fixture_output_df_index(data_frame_0, data_frame_1, data_frame_2):
    """Create output data frame from test data with index column specified.

    """
    return jc.get_output_df([data_frame_0, data_frame_1, data_frame_2],
                            ['mr', 'Mrs', 'DR'], 'timestamp')


@pytest.fixture(name='output_df_no_index', scope='module')
def fixture_output_df_no_index(data_frame_0, data_frame_1):
    """Create output data frame from test data with no index column specified.

    """
    return jc.get_output_df([data_frame_0, data_frame_1], ['X', ''])


def test_exit_index_column_exists(data_frame_0, data_frame_1, data_frame_2):
    """Test exit if specified index column doesn't exist for all data frames.

    """
    with pytest.raises(SystemExit):
        jc.get_output_df([data_frame_0, data_frame_1, data_frame_2],
                         ['', '', ''], 'str_col')


def test_exit_no_index_row_counts_unequal(data_frame_0, data_frame_1,
                                          data_frame_2):
    """Test exit if no index column is specified and row counts are unequal.

    """
    with pytest.raises(SystemExit):
        jc.get_output_df([data_frame_0, data_frame_1, data_frame_2],
                         ['', '', ''])


def test_output_df_index_dimensions(output_df_index):
    """Test whether the data frame shape is correct with an index column
    specified.

    """
    assert output_df_index.shape == (3, 13)


def test_output_df_index_index_values(output_df_index):
    """Test whether the index column values are correct with an index column
    specified.

    """
    assert np.array_equal(output_df_index['timestamp'], [103.0, 104.0, 105.0])


def test_output_df_index_str_values(output_df_index):
    """Test whether string values are correct with an index column specified.

    """
    assert np.array_equal(output_df_index['DR_str_col_0'],
                          ['xyz', 'Xyz', 'XYZ'])


def test_output_df_index_float_values(output_df_index):
    """Test whether float values are correct with an index column specified.

    """
    assert np.array_equal(output_df_index['Mrs_float_col'], [61.1, 72.2, 83.3])


def test_output_df_index_int_values(output_df_index):
    """Test whether int values are correct with an index column specified.

    """
    assert np.array_equal(output_df_index['mr_int_col'], [3, 4, 5])


def test_output_df_no_index_dimensions(output_df_no_index):
    """Test whether the data frame shape is correct with no index column
    specified.

    """
    assert output_df_no_index.shape == (5, 8)


def test_output_df_no_index_str_values(output_df_no_index):
    """Test whether string values are correct with no index column specified.

    """
    assert np.array_equal(output_df_no_index['X_str_col'],
                          ['A', 'BB', 'CCC', 'DDDD', 'EEEEE'])


def test_output_df_no_index_float_values(output_df_no_index):
    """Test whether float values are correct with no index column specified.

    """
    assert np.array_equal(output_df_no_index['X_timestamp'],
                          [101.0, 102.0, 103.0, 104.0, 105.0])


def test_output_df_no_index_int_values(output_df_no_index):
    """Test whether int values are correct with no index column specified.

    """
    assert np.array_equal(output_df_no_index['int_col'], [6, 7, 8, 9, 0])
