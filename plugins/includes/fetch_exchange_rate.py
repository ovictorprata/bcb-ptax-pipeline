import pandas as pd
import requests
from datetime import datetime, timedelta

def adjust_start_date(start_date: str) -> str:
    """Subtracts 7 days from the given start date."""
    return (datetime.strptime(start_date, '%m-%d-%Y') - timedelta(days=7)).strftime('%m-%d-%Y')

def fetch_quotation_data(adjusted_start_date: str, end_date: str) -> pd.DataFrame:
    """Fetches USD exchange rate data from the Brazilian Central Bank."""
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
    df_quotations = df_quotations.rename(columns={
        'cotacaoCompra': 'buy_price',
        'cotacaoVenda': 'sell_price',
        'dataHoraCotacao': 'quotation_datetime'
    })
    df_quotations['quotation_datetime'] = pd.to_datetime(df_quotations['quotation_datetime'])
    
    return df_quotations

def merge_dataframes(adjusted_start_date: str, end_date: str, df_quotations: pd.DataFrame) -> pd.DataFrame:
    """Merges quotation data with a complete daily date range, forward filling missing entries."""
    all_dates = pd.date_range(start=adjusted_start_date, end=end_date)
    df_all = pd.DataFrame({'date': all_dates})

    df_quotations['date'] = df_quotations['quotation_datetime'].dt.floor('D')
    df_complete = pd.merge(df_all, df_quotations, on='date', how='left')
    df_complete.sort_values('date', inplace=True)
    df_complete[['buy_price', 'sell_price', 'quotation_datetime']] = (
        df_complete[['buy_price', 'sell_price', 'quotation_datetime']].ffill()
    )
    
    return df_complete

def filter_dataframe(df_complete: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Filters the merged DataFrame to only include rows within the original date range."""
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    df_complete = df_complete[(df_complete['date'] >= start_date) & (df_complete['date'] <= end_date)]
    df_complete.reset_index(drop=True, inplace=True)
    
    return df_complete

def get_quotation_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Coordinates fetching, merging, and filtering of exchange rate data."""
    adjusted_start_date = adjust_start_date(start_date)
    df_quotations = fetch_quotation_data(adjusted_start_date, end_date)
    df_complete = merge_dataframes(adjusted_start_date, end_date, df_quotations)
    df_complete = filter_dataframe(df_complete, start_date, end_date)
    
    return df_complete

start_date = '02-01-2025'
end_date = '02-31-2025'
df_complete = get_quotation_data(start_date, end_date)

print(df_complete.head(31))
