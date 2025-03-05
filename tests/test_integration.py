import os
import tempfile
import pandas as pd
import xml.etree.ElementTree as ET
import pytest

from src.components.data_ingestion import DataIngestion
from src.components.data_cleaner import DataCleaner

@pytest.fixture
def temp_paths(tmp_path):
    raw_file = tmp_path / "raw.xml"
    clone_file = tmp_path / "clone.xml"
    csv_file = tmp_path / "data.csv"
    return str(raw_file), str(clone_file), str(csv_file)

@pytest.fixture
def xml_factory(tmp_path):
    xml_content = """
    <catalog>
    <book 
        id='bk101' genre='Computer' price='44.95' pub_date='2000-10-01' rating='good' availability='available'>
    </book>
    
    <book 
        id='bk102' genre='Thriller' price='95' publish_date='2010-01-09' rating='bad' availability='not available'>
    </book>
</catalog>
"""

    xml_file = tmp_path/"sample.xml"
    xml_file.write_text(xml_content)
    return xml_file

def test_ingestion_with_cleaner(xml_factory, temp_paths, monkeypatch):
    raw_file, clone_file, csv_file = temp_paths
    child_elem = 'book'

    sample_xml_content = xml_factory.read_text()
    with open(raw_file, 'w') as f:
        f.write(sample_xml_content)



    data_ingestion = DataIngestion()
    
    data_ingestion.ingestion_config.child_elem = child_elem
    data_ingestion.ingestion_config.clone_file_path = clone_file
    data_ingestion.ingestion_config.raw_file_path = raw_file
    data_ingestion.ingestion_config.csv_file_path = csv_file
    

    ingested_paths = data_ingestion.initiate_data_ingestion()
    
    assert os.path.exists(clone_file)
    assert os.path.exists(csv_file)
    assert os.path.exists(raw_file)

    print(ingested_paths)

    df_ingested = pd.read_csv(csv_file)
    
    assert not df_ingested.empty
    assert 'price' in df_ingested.columns
    assert 'genre' in df_ingested.columns

    clean_file_path = csv_file.replace('.csv', '_clean.csv')
    cleaner = DataCleaner(data_path=csv_file, clean_obj_path=clean_file_path)
    
    filter = ('genre',['computer'])
    remove_cols = ['rating', 'availability']
    cols_and_types = [('publish_date', "datetime64[ns]")]

    df_cleaned = cleaner.initiate_data_cleaning(filter=filter, remove_cols=remove_cols, cols_and_types=cols_and_types)
    
    assert os.path.exists(clean_file_path)
    assert 'rating' not in df_cleaned.columns  
    assert 'availability' not in df_cleaned.columns
    assert len(df_cleaned) < 2
    assert df_cleaned['publish_date'].dtype == "datetime64[ns]"