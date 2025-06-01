import pandas as pd
import requests
from datetime import datetime, timedelta

def adjust_start_date(start_date: str) -> str:
    return (datetime.strptime(start_date, '%m-%d-%Y') - timedelta(days=7)).strftime('%m-%d-%Y')

def fetch_quotation_data(adjusted_start_date: str, end_date: str) -> pd.DataFrame:
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
        f"@dataInicial='{adjusted_start_date}'&@dataFinalCotacao='{end_date}'&"
        "$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
    )
    
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()["value"]
    
    df_quotations = pd.DataFrame(data)
    df_quotations['dataHoraCotacao'] = pd.to_datetime(df_quotations['dataHoraCotacao']).dt.date
    df_quotations = df_quotations.rename(columns={
        'cotacaoCompra': 'compra',
        'cotacaoVenda': 'venda',
        'dataHoraCotacao': 'data'
    })
    df_quotations['data'] = pd.to_datetime(df_quotations['data'])
    
    # Adiciona a coluna 'atualizado_em' com a data de cada cotação
    df_quotations['atualizado_em'] = df_quotations['data']
    
    return df_quotations

def merge_dataframes(adjusted_start_date: str, end_date: str, df_quotations: pd.DataFrame) -> pd.DataFrame:
    all_dates = pd.date_range(start=adjusted_start_date, end=end_date)
    df_all = pd.DataFrame({'data': all_dates})
    
    df_complete = pd.merge(df_all, df_quotations, on='data', how='left')
    df_complete[['compra', 'venda']] = df_complete[['compra', 'venda']].ffill()
    
    return df_complete

def filter_dataframe(df_complete: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    df_complete = df_complete[(df_complete['data'] >= start_date) & (df_complete['data'] <= end_date)]
    df_complete.reset_index(drop=True, inplace=True)
    
    return df_complete

def get_quotation_data(start_date: str, end_date: str) -> pd.DataFrame:
    adjusted_start_date = adjust_start_date(start_date)
    df_quotations = fetch_quotation_data(adjusted_start_date, end_date)
    df_complete = merge_dataframes(adjusted_start_date, end_date, df_quotations)
    df_complete = filter_dataframe(df_complete, start_date, end_date)
    
    return df_complete

start_date = '04-07-2025'
end_date = '04-21-2025'
df_complete = get_quotation_data(start_date, end_date)

print(df_complete.head(20))
