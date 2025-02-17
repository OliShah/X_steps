# Create Data Cleaning Class
# Acceptance Criteria:
# Column removal method that allows user to specify columns to remove
# Should accept a df or a csv as input from the data ingestion class
# Create methods to clean the data based on data-scientists' parameters
# The class should be resusable for different datasets, eg. weather
# Saves cleaned dataframe to disk as a csv for debugging
# Stores df as a variable to be passed on to preprocessor class

import os
import sys 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append('src')

from pathlib import Path
import pandas as pd
from dataclasses import dataclass
from logger import logging

@dataclass 
class DataCleanerConfig:
    data_path: Path = Path(r'C:\Users\olish\Documents\ml_projects\X_steps\artefacts') / 'csv.csv'
    clean_obj_path: Path = Path(r'C:\Users\olish\Documents\ml_projects\X_steps\artefacts') / 'clean_data.csv'
    

class DataCleaner:
    def __init__(self, data_path=None, clean_obj_path=None):
        self.data_cleaning_config = DataCleanerConfig()
        self.data_path = data_path if data_path is not None else self.data_cleaning_config.data_path
        self.clean_obj_path = clean_obj_path if clean_obj_path is not None else self.data_cleaning_config.clean_obj_path

    def initiate_data_cleaning(self, filter=None, remove_cols=None):
        """
        Cleans data and optionally applies a filter based on a column and a list of keyword(s).

        Parameters:
        - datapath: path to the csv to be cleaned
        - filter: A tuple (column, keyword_list) to filter rows where the column contains the keyword.
              If None, no filtering is applied.
        - remove_cols: a list of columns to be removed. If None, column removal is not applied.

        Returns:
        - Cleaned and optionally filtered DataFrame.
        """
        logging.info("Data cleanining initiated.")

        try:
            df = pd.read_csv(self.data_path)
            logging.info("csv read as dataframe.")

            if filter is not None:
                df = self.filter_col(df, filter)
            logging.info("filter is accessible")

            if remove_cols is not None:
                df = self.remove_cols(df, remove_cols=remove_cols)  # Replace 'column_to_remove' with actual column names
            logging.info("column remover is accessesible.")

#           df.to_csv(self.clean_obj_path, index=False)
#            logging.info(f"Cleaned data saved to {self.clean_obj_path}")
            
            return df

        except Exception as e:
            logging.error(f"Error occurred during data cleaning: {e}")
            raise e

    def filter_col(self, df, filter):

        if not isinstance(filter, list):
            logging.error(f"Expected list for cols, got {type(filter).__name__}")
            raise TypeError(f"Expected list for cols, got {type(filter).__name__}")
        
        col, keyword_list = filter
        return df[df[col].isin(keyword_list)]
    
    def remove_cols(self, df, cols):
        """
        Removes specified columns from the DataFrame.

        Parameters:
        - df: The DataFrame from which columns are to be removed.
        - cols: A list of column names to be removed.

        Returns:
        - DataFrame with specified columns removed.
        """

        if not isinstance(cols, list):
            logging.error(f"Expected list for cols, got {type(cols).__name__}")
            raise TypeError(f"Expected list for cols, got {type(cols).__name__}")
    
        try:
            df = df.drop(columns=cols)
            logging.info(f"Columns {cols} removed from dataframe.")
            return df
        except KeyError as e:
            logging.error(f"Error occurred while removing columns: {e}")
            raise e

if __name__ == '__main__':
    try:
        cleaning_obj = DataCleaner()
        filter = ("type", ['HKQuantityTypeIdentifierStepCount'])
        clean_data = cleaning_obj.initiate_data_cleaning(filter=filter)
        #clean_data.to_csv(cleaning_obj.clean_obj_path)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

