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
