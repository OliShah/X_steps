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
    child_elem: str = os.getenv('CHILD_ELEM')

    

# Splitting time series data:
# https://medium.com/@mouadenna/time-series-splitting-techniques-ensuring-accurate-model-validation-5a3146db3088

class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("Data ingestion initiated")

        raw_file_path = self.ingestion_config.raw_file_path
        clone_file_path = self.ingestion_config.clone_file_path
        csv_file_path = self.ingestion_config.csv_file_path
        child_elem = self.ingestion_config.child_elem

        logging.info(f"Raw file path: {raw_file_path}")
        logging.info(f"Clone file path: {clone_file_path}")
        logging.info(f"CSV file path: {csv_file_path}")
        logging.info(f"Child element: {child_elem}")

        try:
            clone_data(raw_file_path, self.ingestion_config)
            clone_to_csv(clone_file_path, self.ingestion_config, child_elem)
            
            return (
                self.ingestion_config.raw_file_path,
                self.ingestion_config.clone_file_path,
                self.ingestion_config.csv_file_path
            )

        except FileNotFoundError as e:
            logging.error(f"File not found: {e}")
            raise CustomException(e, sys)
        except et.ParseError as e:
            logging.error(f"Error parsing XML fileat {raw_file_path}: {e}")
            raise CustomException(e, sys)
        except ValueError as e:
            logging.error(f"Value error: {e}")
            raise CustomException(e, sys)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise CustomException(e, sys)
        
    
def clone_data(raw_file_path, ingestion_config, et=et ):
    tree = et.parse(raw_file_path)
    root = tree.getroot()
    logging.info('Raw file retrieved')

    os.makedirs(os.path.dirname(ingestion_config.clone_file_path), exist_ok=True)
    logging.info("Raw file cloned successfully.")
    tree.write(ingestion_config.clone_file_path)

def clone_to_csv(clone_file_path, ingestion_config, child_elem, et=et):
    try:
        tree = et.parse(clone_file_path)
        root = tree.getroot()
        logging.info(f'Clone file retrieved from {clone_file_path}')

        data = []

        elements = root.findall(child_elem)
        if not elements:
            raise ValueError(f"No elements found with tag '{child_elem}' in the XML file.")

        for elem in elements:
                data.append(elem.attrib)

        df = pd.DataFrame(data)
        df.to_csv(ingestion_config.csv_file_path, index=False)
        logging.info(f'Clone to CSV complete: {ingestion_config.csv_file_path}')

    except FileNotFoundError:
        logging.error(f"File not found:  {clone_file_path}")
    except et.ParseError:
        logging.error(f'Error parsing XML file: {clone_file_path}')
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')

if __name__ == "__main__":
    obj=DataIngestion()
    obj.initiate_data_ingestion()

