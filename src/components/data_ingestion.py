import os 
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append('src')

from exception import CustomException
from logger import logging

import pandas as pd
from dataclasses import dataclass
import xml.etree.ElementTree as et

from dotenv import load_dotenv

load_dotenv()

@dataclass
class DataIngestionConfig:
    clone_file_path: str = os.getenv('CLONE_FILE_PATH')
    csv_file_path: str = os.getenv('CSV_FILE_PATH')
    raw_file_path: str = os.getenv('RAW_FILE_PATH')

# Splitting time series data:
# https://medium.com/@mouadenna/time-series-splitting-techniques-ensuring-accurate-model-validation-5a3146db3088

class DataIngestion:
    def __init__(self):
        self.ingestion_config=DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Data ingestion initiated")

        raw_file_path = self.ingestion_config.raw_file_path
        clone_file_path = self.ingestion_config.clone_file_path
        logging.info(f"raw file path: {raw_file_path}")
        csv_file_path = self.ingestion_config.csv_file_path
        logging.info(f"Clone file path: {clone_file_path}")

        if raw_file_path is None:
            raise ValueError("Raw file path is not set. Check the environment variable.")
        
        try: 
            clone_data(raw_file_path, self.ingestion_config)
            clone_to_csv(clone_file_path, self.ingestion_config)

            return (
                self.ingestion_config.raw_file_path,
                self.ingestion_config.clone_file_path
                )
           
        except Exception as e:
            raise CustomException(e, sys)
        
    
def clone_data(raw_file_path, ingestion_config, et=et ):
    tree = et.parse(raw_file_path)
    root = tree.getroot()
    logging.info('Raw file retrieved')

    os.makedirs(os.path.dirname(ingestion_config.clone_file_path), exist_ok=True)
    logging.info("Raw file cloned successfully.")
    tree.write(ingestion_config.clone_file_path)

def clone_to_csv(clone_file_path, ingestion_config, et=et):
    tree = et.parse(clone_file_path)
    root = tree.getroot()
    logging.info('Clone file retrieved')

    data = []

    for record in root.findall('Record'):
        data.append(record.attrib)

    df = pd.DataFrame(data)
    df.to_csv(ingestion_config.csv_file_path, index=False)
    logging.info('clone to csv complete')


if __name__ == "__main__":
    obj=DataIngestion()
    obj.initiate_data_ingestion()

