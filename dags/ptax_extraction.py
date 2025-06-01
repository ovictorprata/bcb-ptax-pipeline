from datetime import datetime, date, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pandas.tseries.offsets import BMonthBegin

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_first_business_day_next_month():
    """Retorna o primeiro dia útil do mês seguinte"""
    return (date.today() + BMonthBegin(1)).strftime('%Y-%m-%d')

def format_date_for_api(date_obj):
    return date_obj.strftime('%m-%d-%Y')

def fetch_ptax_data():
    today = date.today()
    start_date = end_date = today
    
    url = (
        f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
        f"@dataInicial='{format_date_for_api(start_date)}'&@dataFinalCotacao='{format_date_for_api(end_date)}'&"
        f"$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
    )
    
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return pd.DataFrame(response.json().get('value', []))

def process_and_save_data():
    # Extrai dados da API
    df = fetch_ptax_data()
    if df.empty:
        raise ValueError("Nenhum dado retornado pela API")
    
    # Processamento básico
    df['data'] = pd.to_datetime(df['dataHoraCotacao']).dt.date
    df['atualizado_em'] = datetime.now().date()
    df.rename(columns={
        'cotacaoCompra': 'compra',
        'cotacaoVenda': 'venda'
    }, inplace=True)

    # 1. Dados para Receita (congelados no 1º dia útil do mês seguinte)
    first_business_day = get_first_business_day_next_month()
    df_receita = df[df['data'] < pd.to_datetime(first_business_day).date()]
    
    # 2. Dados para Mesa de Câmbio (atualizáveis por 10 dias)
    cutoff_date = date.today() - timedelta(days=10)
    df_mesa = df[df['data'] >= cutoff_date]

    # Salva em Parquet
    df_receita.to_parquet('/opt/airflow/data/ptax_receita.parquet', engine='pyarrow')
    df_mesa.to_parquet('/opt/airflow/data/ptax_mesa_cambio.parquet', engine='pyarrow')

    print(f"Dados salvos: Receita ({len(df_receita)} registros), Mesa ({len(df_mesa)} registros)")

with DAG(
    'ptax_dual_processing',
    default_args=default_args,
    schedule_interval='0 10,15,20 * * *',
    catchup=False,
) as dag:

    process_task = PythonOperator(
        task_id='process_and_save_ptax_data',
        python_callable=process_and_save_data,
    )