import os
import pytest
import pandas as pd
import requests
import tarfile
from io import StringIO
from unittest.mock import patch, mock_open
from airflow.dags.ETL_toll_data import (
    download_dataset,
    untar_dataset,
    extract_data_from_csv,
    extract_data_from_tsv,
    extract_data_from_fixed_width,
    consolidate_data,
    transform_data,
    staging_dir
)

@patch('airflow.dags.ETL_toll_data.requests.get')
@patch('builtins.open', new_callable=mock_open)
def test_download_dataset(mock_open, mock_requests_get):
    mock_response = requests.Response()
    mock_response._content = b'fake data'
    mock_requests_get.return_value = mock_response
    
    download_dataset()
    mock_requests_get.assert_called_once_with("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz")
    mock_open.assert_called_once_with(os.path.join(staging_dir, "tolldata.tgz"), "wb")

@patch('airflow.dags.ETL_toll_data.tarfile.open')
def test_untar_dataset(mock_tarfile_open):
    mock_tarfile = mock_tarfile_open.return_value
    mock_tarfile.is_tarfile.return_value = True
    
    untar_dataset()
    mock_tarfile_open.assert_called_once_with(os.path.join(staging_dir, "tolldata.tgz"), "r:gz")
    mock_tarfile.extractall.assert_called_once_with(staging_dir)

@patch('pandas.read_csv')
def test_extract_data_from_csv(mock_read_csv):
    mock_df = pd.DataFrame({
        'Rowid': [1, 2],
        'Timestamp': ['2021-01-01', '2021-01-02'],
        'Anonymized Vehicle number': ['ABC123', 'XYZ789'],
        'VehicleType': ['Car', 'Truck'],
        'dummy': ['data', 'data'],
        'dummy2': ['data', 'data']
    })
    mock_read_csv.return_value = mock_df
    
    extract_data_from_csv()
    mock_read_csv.assert_called_once_with(os.path.join(staging_dir, "vehicle-data.csv"), names=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'VehicleType', 'dummy', 'dummy2'])
    assert os.path.isfile(os.path.join(staging_dir, 'csv_data.csv'))

# Add similar tests for other functions

if __name__ == "__main__":
    pytest.main()
