from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from datetime import datetime
from includes.load import load_to_parquet
from includes.fetch_exchange_rate import get_quotation_data

def fx_usd_live(**kwargs: Context):  # contexto passado automaticamente
    execution_date = kwargs["logical_date"].date()  # o correto agora é `logical_date`
    start_date = "01-01-2021"  # formato MM-DD-YYYY
    end_date = execution_date.strftime("%m-%d-%Y")

    df = get_quotation_data(start_date, end_date)
    load_to_parquet(df, "data/fx_usd_live.parquet")

    print(f"Dados salvos até {end_date}")

with DAG(
    dag_id="fx_usd_live",
    start_date=datetime(2021, 1, 1),
    schedule="0 10,15,20 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["fx", "live"],
) as dag:
    atualizar_fx_live = PythonOperator(
        task_id="atualizar_fx_usd_live",
        python_callable=fx_usd_live,
    )
