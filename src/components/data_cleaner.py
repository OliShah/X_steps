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

    def initiate_data_cleaning(self, filter=None, remove_cols=None, cols_and_types=None):
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
                df = self.remove_cols(df, remove_cols=remove_cols) 
            logging.info("column remover is accessesible.")

            if cols_and_types is not None:
                df = self.change_col_type(df=df, cols_and_types=cols_and_types)

            df.to_csv(self.clean_obj_path, index=False)
            logging.info(f"Cleaned data saved to {self.clean_obj_path}")
            
            return df

        except Exception as e:
            logging.error(f"Error occurred during data cleaning: {e}")
            raise e

    def change_col_type(self, df, cols_and_types):
        """
        Changes dtype of specified columns from the DataFrame.

        Parameters:
        - df: The DataFrame in which columns are to be converted.
        - cols_and_types: A list which contains at least one tuple [(col, type)] specifying cols and the type to convert to.

        Returns:
        - DataFrame with specified columns' in their new types.
        """

        if not isinstance(cols_and_types, list):
            logging.error(f"Expected list for cols_and_types, got {type(cols_and_types).__name__}")
            raise TypeError(f"Expected list for cols_and_types, got {type(cols_and_types).__name__}")
        
        if not all(isinstance(item, tuple) and len(item) == 2 for item in cols_and_types):
            logging.error(f"Expected tuples of length 2 for cols_and_types, got {type(cols_and_types).__name__}")
            raise TypeError(f"Expected tuples of length 2 in cols_and_types, got {type(cols_and_types).__name__}")
    
        try:
            for item in cols_and_types :
                col, col_type = item
                
                if col_type == "int":
                    df[col] = df[col].astype(int)
                elif col_type == "float":
                    df[col] = df[col].astype(float)
                elif col_type == "object":
                    df[col] = df[col].astype(str)
                elif col_type == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col])
                else:
                    logging.warning(f"Unsupported type '{col_type}' for column '{col}'. Skipping.")
        
        except KeyError as e:
            logging.error(f"Error changing column types: {e}")
            raise 

        return df

    def filter_col(self, df, filter):

        if not isinstance(filter, tuple):
            logging.error(f"Expected tuple for filter, got {type(filter).__name__}")
            raise TypeError(f"Expected tuple for filter, got {type(filter).__name__}")
        
        col, keyword_list = filter
        return df[df[col].isin(keyword_list)]
    
    def remove_cols(self, df, remove_cols):
        """
        Removes specified columns from the DataFrame.

        Parameters:
        - df: The DataFrame from which columns are to be removed.
        - cols: A list of column names to be removed.

        Returns:
        - DataFrame with specified columns removed.
        """

        if not isinstance(remove_cols, list):
            logging.error(f"Expected list for remove_cols, got {type(remove_cols).__name__}")
            raise TypeError(f"Expected list for remove_cols, got {type(remove_cols).__name__}")
    
        try:
            df = df.drop(columns=remove_cols)
            logging.info(f"Columns {remove_cols} removed from dataframe.")
            return df
        except KeyError as e:
            logging.error(f"Error occurred while removing columns: {e}")
            raise e

# if __name__ == '__main__':
#     try:
#         ingestion_obj = DataIngestion()
#         cleaning_obj = DataCleaner()
#         filter = ("type", ['HKQuantityTypeIdentifierStepCount'])
#         remove_cols = ['type','sourceName','sourceVersion','device','unit','creationDate','endDate']
#         clean_data = cleaning_obj.initiate_data_cleaning(df=DataIngestion, filter=filter, remove_cols=cols)
#         clean_data.to_csv(cleaning_obj.clean_obj_path)

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         raise

