# Create Data Cleaning Class
# Acceptance Criteria:
# Column removal method that allows user to specify columns to remove
# Should accept a df or a csv as input from the data ingestion class
# Create methods to clean the data based on data-scientists' parameters
# The class should be resusable for different datasets, eg. weather
# Saves cleaned dataframe to disk as a csv for debugging
# Stores df as a variable to be passed on to preprocessor class

import os
import pandas as pd
from dataclasses import dataclass
from logger import logging

@dataclass 
class DataCleanerConfig:
    clean_obj_file_path: str = os.path.join('artefacts', 'clean_data.csv')

class DataCleaner:
    def __init__(self):
        self.data_cleaning_config = DataCleanerConfig()

    def initiate_data_cleaning(self, data_path, filter=None):
        """
        Cleans data and optionally applies a filter based on a column and a list of keyword(s).

        Parameters:
        - datapath: path to csv
        - filter: A tuple (column, keyword_list) to filter rows where the column contains the keyword.
              If None, no filtering is applied.
        Returns:
        - Cleaned and optionally filtered DataFrame.
        """

        try:
            df = pd.read_csv(data_path)
            logging.info("csv read as dataframe.")

            if filter is not None:
                df = self.filter_col(df, filter)

            return df

        except Exception as e:
            logging.error(f"Error occurred during data cleaning: {e}")
            raise e

    def filter_col(self, df, filter):
        col, keyword_list = filter
        return df[df[col].isin(keyword_list)]

    

