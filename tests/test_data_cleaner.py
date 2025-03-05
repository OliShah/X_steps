import pytest
import pandas as pd
from src.components.data_cleaner import DataCleaner

@pytest.fixture
def data_cleaner():
    return DataCleaner()

@pytest.fixture
def df_factory():
    return pd.DataFrame({
        'A': [1, 2, 3, 4],
        'B': ['foo', 'bar', 'foo', 'baz'],
    })

# Type Conversion-
def test_change_col_nonstring_datetime(data_cleaner, df_factory):
    df_factory["C"] = [2001/11/1, 2002/2/2, 2003/3/2, 2004/4/4]
    
    cols_and_types = [('C', 'datetime64[ns]')]
    datetime_nonstring_df = data_cleaner.change_col_type(df_factory, cols_and_types=cols_and_types)

    assert datetime_nonstring_df['C'].dtype == cols_and_types[0][1]

def test_change_col_datetime_yearfirst(data_cleaner, df_factory):
    df_factory["C"] = ['2001/11/01', '2002/02/02', '2003/3/02', '2004/4/04']
    
    cols_and_types = [('C', 'datetime64[ns]')]
    yearfirst_df = data_cleaner.change_col_type(df_factory, cols_and_types=cols_and_types)

    assert yearfirst_df['C'].dtype == cols_and_types[0][1]


def test_change_col_hyphen_datetime(data_cleaner, df_factory):
    df_factory["C"] = ['1-01-2001', '2-02-2002', '3-02-2003', '4-04-2004']
    
    cols_and_types = [('C', 'datetime64[ns]')]
    hyphen_date_df = data_cleaner.change_col_type(df_factory, cols_and_types=cols_and_types)

    assert hyphen_date_df['C'].dtype == cols_and_types[0][1]

def test_change_col_to_datetime(data_cleaner, df_factory):
    df_factory["C"] = ['1/01/2001', '2/02/2002', '3/02/2003', '4/04/2004']
    
    cols_and_types = [('C', 'datetime64[ns]')]
    datetime_convert_df = data_cleaner.change_col_type(df_factory, cols_and_types=cols_and_types)

    assert datetime_convert_df['C'].dtype == cols_and_types[0][1]

def test_change_col_to_float(data_cleaner, df_factory):
    df_factory["C"] = ['1', '2', '3', '4']
    
    cols_and_types = [('C', 'float')]
    float_convert_df = data_cleaner.change_col_type(df_factory, cols_and_types=cols_and_types)

    assert float_convert_df['C'].dtype == cols_and_types[0][1]

def test_change_col_to_string(data_cleaner, df_factory):
    df_factory["C"] = [1, 2, 3, 4]
    
    cols_and_types = [('C', 'object')]
    string_convert_df = data_cleaner.change_col_type(df_factory, cols_and_types=cols_and_types)

    assert string_convert_df['C'].dtype == cols_and_types[0][1]

# Remove Cols    
def test_remove_col_with_type_error(data_cleaner, df_factory):
    remove_cols = 'A'

    with pytest.raises(TypeError):
        data_cleaner.remove_cols(df_factory, remove_cols=remove_cols)

def test_remove_nonexistent_col(data_cleaner, df_factory):
    remove_cols = ['Z']

    with pytest.raises(KeyError):
        data_cleaner.remove_cols(df_factory, remove_cols=remove_cols)

def test_remove_more_than_one_cols(data_cleaner, df_factory):
    remove_cols = ['A', 'B']
    removed_col_df = data_cleaner.remove_cols(df_factory, remove_cols=remove_cols)

    expected_df = df_factory[[]]
    pd.testing.assert_frame_equal(removed_col_df, expected_df)

def test_remove_cols(data_cleaner, df_factory):
    remove_cols = ['A']
    removed_col_df = data_cleaner.remove_cols(df_factory, remove_cols=remove_cols)

    expected_df = df_factory[['B']]
    pd.testing.assert_frame_equal(removed_col_df, expected_df)

#Filter cols
def test_filter_col(data_cleaner, df_factory):
    filter = ('B', ['foo'])
    filtered_df = data_cleaner.filter_col(df_factory, filter)
    
    expected_df = df_factory[df_factory['B'].isin(['foo'])]
    pd.testing.assert_frame_equal(filtered_df, expected_df)

    filter = ('A', [9])
    filtered_df = data_cleaner.filter_col(df_factory, filter)
    expected_df = df_factory[df_factory['A'].isin([9])]
    pd.testing.assert_frame_equal(filtered_df, expected_df)

    filter = ('B', ['fo'])
    filtered_df = data_cleaner.filter_col(df_factory, filter)
    expected_df = df_factory[df_factory['B'].isin(['fo'])]
    pd.testing.assert_frame_equal(filtered_df, expected_df)

    with pytest.raises(KeyError):
        filter = ('C', ['foo'])
        data_cleaner.filter_col(df_factory, filter)
