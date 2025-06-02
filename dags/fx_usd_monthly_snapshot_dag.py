from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator, get_current_context
from datetime import datetime, timedelta, date
from pendulum import timezone

from includes.business_days import FIRST_BUSINESS_DAYS
from includes.fetch_exchange_rate import get_quotation_data
from includes.load import load_to_parquet

def is_first_business_day() -> bool:
    context = get_current_context()
    logical_date: date = context["logical_date"].date()

    expected_first_biz_day = FIRST_BUSINESS_DAYS.get((logical_date.year, logical_date.month))
    if expected_first_biz_day is None:
        print(f"⚠️ Chave {(logical_date.year, logical_date.month)} não encontrada em FIRST_BUSINESS_DAYS. Pulando a execução.")
        return False

    if logical_date == expected_first_biz_day:
        print(f"✅ {logical_date} é o primeiro dia útil de {logical_date.month}/{logical_date.year}. Prosseguindo com a execução.")
        return True
    else:
        print(f"⏭️ {logical_date} não é o primeiro dia útil de {logical_date.month}/{logical_date.year} (esperado: {expected_first_biz_day}). Pulando.")
        return False

def generate_snapshot():
    context = get_current_context()
    logical_date: date = context["logical_date"].date()

    first_day_this_month = logical_date.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)

    start_date_str = first_day_prev_month.strftime("%m-%d-%Y")
    end_date_str = last_day_prev_month.strftime("%m-%d-%Y")

    print(f"📅 Gerando snapshot de {start_date_str} até {end_date_str}...")

    df = get_quotation_data(start_date_str, end_date_str)

    output_path = (
        f"data/fx_usd_snapshot_"
        f"{first_day_prev_month.year}_"
        f"{first_day_prev_month.month:02d}.parquet"
    )

    load_to_parquet(df, output_path)

    print(f"✅ Snapshot salvo em: {output_path}")

with DAG(
    dag_id="fx_usd_monthly_snapshot",
    start_date=datetime(2021, 1, 1, tzinfo=timezone("America/Sao_Paulo")),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["fx", "monthly", "snapshot"],
) as dag:

    check_first_biz = ShortCircuitOperator(
        task_id="is_first_business_day",
        python_callable=is_first_business_day,
    )

    create_snapshot = PythonOperator(
        task_id="generate_monthly_fx_snapshot",
        python_callable=generate_snapshot,
    )

    check_first_biz >> create_snapshot
