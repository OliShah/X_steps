import pytest
import pandas as pd
from src.components.data_cleaner import DataCleaner

@pytest.fixture
def data_cleaner():
    return DataCleaner()

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'A': [1, 2, 3, 4],
        'B': ['foo', 'bar', 'foo', 'baz']
    })

def test_remove_col_with_type_error(data_cleaner, sample_df):
    remove_cols = 'A'

    with pytest.raises(TypeError):
        data_cleaner.remove_cols(sample_df, cols=remove_cols)

def test_remove_nonexistent_col(data_cleaner, sample_df):
    remove_cols = ['C']

    with pytest.raises(KeyError):
        data_cleaner.remove_cols(sample_df, cols=remove_cols)

def test_remove_more_than_one_cols(data_cleaner, sample_df):
    remove_cols = ['A', 'B']
    removed_col_df = data_cleaner.remove_cols(sample_df, cols=remove_cols)

    expected_df = sample_df[[]]
    pd.testing.assert_frame_equal(removed_col_df, expected_df)

def test_remove_cols(data_cleaner, sample_df):
    remove_cols = ['A']
    removed_col_df = data_cleaner.remove_cols(sample_df, cols=remove_cols)

    expected_df = sample_df[['B']]
    pd.testing.assert_frame_equal(removed_col_df, expected_df)

def test_filter_col(data_cleaner, sample_df):
    filter = ('B', ['foo'])
    filtered_df = data_cleaner.filter_col(sample_df, filter)
    
    expected_df = sample_df[sample_df['B'].isin(['foo'])]
    pd.testing.assert_frame_equal(filtered_df, expected_df)

    filter = ('A', [9])
    filtered_df = data_cleaner.filter_col(sample_df, filter)
    expected_df = sample_df[sample_df['A'].isin([9])]
    pd.testing.assert_frame_equal(filtered_df, expected_df)

    filter = ('B', ['fo'])
    filtered_df = data_cleaner.filter_col(sample_df, filter)
    expected_df = sample_df[sample_df['B'].isin(['fo'])]
    pd.testing.assert_frame_equal(filtered_df, expected_df)

    with pytest.raises(KeyError):
        filter = ('C', ['foo'])
        data_cleaner.filter_col(sample_df, filter)
