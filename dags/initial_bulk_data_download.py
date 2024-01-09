from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from etl.extract import download_curtailment_historical_data, download_eeg_historical_data
from etl.load import database_setup, insert_with_temp_table
from etl.preprocess import etl_curtailment_data, etl_EEG_data, merge_and_calculate

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id = 'initial_bulk_data_download',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bulk']
)
def bulk_data_download_dag():
    @task
    def download_and_process_curtailment_data():
        df_curtailment = download_curtailment_historical_data('2022-12-31')
        df_curtailment_etl = etl_curtailment_data(df_curtailment)
        return df_curtailment_etl

    @task
    def download_and_process_eeg_data():
        df_eeg = download_eeg_historical_data()
        df_eeg_etl = etl_EEG_data(df_eeg)
        return df_eeg_etl

    @task
    def merge_and_load_data(df_curtailment_etl, df_eeg_etl):
        df_ready = merge_and_calculate(df_curtailment_etl, df_eeg_etl)
        database_setup()  # Setup database
        insert_with_temp_table(df_ready, db_name="power_data.db", table_name="Curtailment")

    df_curtailment_etl = download_and_process_curtailment_data()
    df_eeg_etl = download_and_process_eeg_data()

    merge_and_load_data(df_curtailment_etl, df_eeg_etl)

# Instantiate the DAG
bulk_data_download_dag_instance = bulk_data_download_dag()
