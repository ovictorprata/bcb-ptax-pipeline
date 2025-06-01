from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from includes.ptax_utils import fetch_ptax_data, fill_missing_quotes
from pathlib import Path


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def collect_ptax_quotes():
    df_raw = fetch_ptax_data()
    df_filled = fill_missing_quotes(df_raw, days=10)

    output_path = Path('/opt/airflow/data/ptax/')
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / 'ptax_last_10_days.parquet'
    df_filled.to_parquet(file_path, index=False)
    print(f'PTAX quotes saved to {file_path}')


with DAG(
    dag_id='ptax_last_10_days_collector',
    default_args=default_args,
    schedule='0 10,15,20 * * *', 
    catchup=False,
    tags=['ptax', 'dollar', 'quotes']
) as dag:

    fetch_and_save = PythonOperator(
        task_id='fetch_and_save_ptax_last_10_days',
        python_callable=collect_ptax_quotes
    )

    fetch_and_save
