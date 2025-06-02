import pandas as pd

def transform_dolar_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Cleans and renames columns."""
    df = df_raw.rename(columns={
        'cotacaoCompra': 'buy_price',
        'cotacaoVenda': 'sell_price',
        'dataHoraCotacao': 'quotation_datetime'
    })
    df['quotation_datetime'] = pd.to_datetime(df['quotation_datetime'])
    df['date'] = df['quotation_datetime'].dt.floor('D')
    return df

def generate_complete_series(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """Merges quotations with complete date range and applies forward fill."""
    all_dates = pd.date_range(start=df['date'].min(), end=end)
    df_all = pd.DataFrame({'date': all_dates})
    df_merged = pd.merge(df_all, df, on='date', how='left')
    df_merged.sort_values('date', inplace=True)
    df_merged[['buy_price', 'sell_price', 'quotation_datetime']] = df_merged[
        ['buy_price', 'sell_price', 'quotation_datetime']
    ].ffill()
    return df_merged
