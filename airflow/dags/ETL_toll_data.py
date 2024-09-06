import os
import pandas as pd
import requests
import tarfile
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# Define the default arguments for the DAG
default_args = {
    'owner': 'dummy_owner', 
    'start_date': datetime.today(),  
    'email': ['dummy_email@example.com'], 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'ETL_toll_data', 
    default_args=default_args, 
    description='An ETL pipeline to process toll data', 
    schedule_interval='@daily', 
    catchup=False
)

# Paths
source_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
staging_dir = "/tmp/ETL_toll_data_staging"

# Ensure the destination directory exists
os.makedirs(staging_dir, exist_ok=True)

# Set up logging
logger = logging.getLogger(__name__)

# Python functions

def download_dataset(**kwargs):
    """
    Function to download the dataset
    """
    logger.info("Starting to download the dataset from %s", source_url)
    try:
        response = requests.get(source_url)
        response.raise_for_status()
        file_path = os.path.join(staging_dir, "tolldata.tgz")
        
        with open(file_path, "wb") as f:
            f.write(response.content)
        
        logger.info("Dataset downloaded successfully to %s", file_path)
    except Exception as e:
        logger.error("Failed to download the dataset: %s", str(e))
        raise

def untar_dataset(**kwargs):
    """
    Function to untar the dataset
    """
    file_path = os.path.join(staging_dir, "tolldata.tgz")
    logger.info("Starting to untar the dataset at %s", file_path)
    
    try:
        if tarfile.is_tarfile(file_path):
            with tarfile.open(file_path, "r:gz") as tar:
                tar.extractall(staging_dir)
            logger.info("Dataset untarred successfully to %s", staging_dir)
        else:
            logger.error("%s is not a valid tarfile", file_path)
            raise ValueError("Not a tarfile")
    except Exception as e:
        logger.error("Failed to untar the dataset: %s", str(e))
        raise

def extract_data_from_csv(**kwargs):
    """
    Function to extract data from the CSV file
    """
    csv_file = os.path.join(staging_dir, "vehicle-data.csv")
    logger.info("Starting to extract data from CSV file at %s", csv_file)
    
    try:
        df = pd.read_csv(csv_file, names=['Rowinf', 'Timestamp', 'Anonymized Vehicle number', 'VehicleType', 'dummy', 'dummy2'])
        df_filtered = df[['Rowinf', 'Timestamp', 'Anonymized Vehicle number', 'VehicleType']]
        output_path = os.path.join(staging_dir, 'csv_data.csv')
        df_filtered.to_csv(output_path, index=False)
        logger.info("CSV data saved successfully to %s", output_path)
    except Exception as e:
        logger.error("Failed to extract data from CSV file: %s", str(e))
        raise

def extract_data_from_tsv(**kwargs):
    """
    Function to extract data from the TSV file
    """
    tsv_file = os.path.join(staging_dir, "tollplaza-data.tsv")
    logger.info("Starting to extract data from TSV file at %s", tsv_file)
    
    try:
        df = pd.read_csv(tsv_file, sep="\t", names=['Number of axles', 'Tollplaza id', 'Tollplaza code'])
        output_path = os.path.join(staging_dir, 'tsv_data.csv')
        df_filtered = df[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
        df_filtered.to_csv(output_path, index=False)
        logger.info("TSV data saved successfully to %s", output_path)
    except Exception as e:
        logger.error("Failed to extract data from TSV file: %s", str(e))
        raise

def extract_data_from_fixed_width(**kwargs):
    """
    Function to extract data from the fixed width file
    """
    fixed_width_file = os.path.join(staging_dir, "payment-data.txt")
    logger.info("Starting to extract data from fixed width file at %s", fixed_width_file)
    
    try:
        colspecs = [(61, 64), (65, 70)]
        column_names = ['Type of Payment code', 'Vehicle Code']
        df = pd.read_fwf(fixed_width_file, colspecs=colspecs, header=None, names=column_names)
        output_path = os.path.join(staging_dir, 'fixed_width_data.csv')
        df.to_csv(output_path, index=False)
        logger.info("Fixed width data saved successfully to %s", output_path)
    except Exception as e:
        logger.error("Failed to extract data from fixed width file: %s", str(e))
        raise

def consolidate_data(**kwargs):
    """
    Function to consolidate the extracted data
    """
    logger.info("Starting to consolidate data")
    
    try:
        csv_data = pd.read_csv(os.path.join(staging_dir, 'csv_data.csv'))
        tsv_data = pd.read_csv(os.path.join(staging_dir, 'tsv_data.csv'))
        fixed_width_data = pd.read_csv(os.path.join(staging_dir, 'fixed_width_data.csv'))
        consolidated_df = pd.concat([csv_data, tsv_data, fixed_width_data], axis=1)
        output_path = os.path.join(staging_dir, 'extracted_data.csv')
        consolidated_df.to_csv(output_path, index=False)
        logger.info("Consolidated data saved successfully to %s", output_path)
    except Exception as e:
        logger.error("Failed to consolidate data: %s", str(e))
        raise

def transform_data(**kwargs):
    """
    Function to transform the consolidated data
    """
    logger.info("Starting data transformation")
    
    try:
        consolidated_data = pd.read_csv(os.path.join(staging_dir, 'extracted_data.csv'))
        consolidated_data['VehicleType'] = consolidated_data['VehicleType'].str.upper()
        output_path = os.path.join(staging_dir, 'transformed_data.csv')
        consolidated_data.to_csv(output_path, index=False)
        logger.info("Transformed data saved successfully to %s", output_path)
    except Exception as e:
        logger.error("Failed to transform data: %s", str(e))
        raise


# Define the tasks
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag
)

untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag
)

extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag
)

extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag
)

extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag
)

consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# Set task dependencies
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
