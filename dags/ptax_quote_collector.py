from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from includes.ptax_utils import fetch_ptax_data, fill_missing_quotes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def etl_ptax_10_days_to_db():
    print("Iniciando ETL PTAX (últimos 10 dias).")

    try:
        df_raw = fetch_ptax_data()
        print(f"PTAX bruto: {len(df_raw)} linhas.")
    except requests.exceptions.RequestException as e:
        print(f"Erro de rede ao buscar dados PTAX: {e}")
        raise
    except Exception as e:
        print(f"Erro ao obter dados PTAX: {e}")
        raise

    if df_raw.empty:
        print("Nenhum dado retornado da API.")
        return

    try:
        df_filled = fill_missing_quotes(df_raw, days=10)
        print(f"Dados após preenchimento: {len(df_filled)} linhas.")
    except Exception as e:
        print(f"Erro ao preencher cotações: {e}")
        raise

    if df_filled.empty:
        print("Nenhum dado após preenchimento.")
        return

    df_filled['date'] = pd.to_datetime(df_filled['date'])

    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_data_conn")
        engine = pg_hook.get_sqlalchemy_engine()

        df_filled.to_sql(
            name="mesa_cambio_quotations",
            con=engine,
            if_exists='replace',
            index=False
        )
        print("Dados salvos com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")
        raise

with DAG(
    dag_id='ptax_10_dias_to_db',
    schedule='0 10,15,20 * * *',
    catchup=False,
    default_args=default_args,
    tags=['ptax', 'cambio', 'database']
) as dag:

    run_etl_task = PythonOperator(
        task_id='run_etl_ptax_10_days_to_db',
        python_callable=etl_ptax_10_days_to_db,
    )

    run_etl_task
