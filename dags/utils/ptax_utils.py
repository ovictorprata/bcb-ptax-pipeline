import pandas as pd
from datetime import date, datetime, timedelta
import requests


def format_date_for_api(d):
    '''Convert date to MM-DD-YYYY format for Olinda API'''
    return d.strftime('%m-%d-%Y')


def fetch_ptax_data(end_date=None):
    '''
    Fetches PTAX exchange rate data from the Olinda API.
    If no end_date is provided, uses today.
    Queries the last 16 calendar days from end_date.
    '''
    if end_date is None:
        end_date = date.today()

    start_date = end_date - timedelta(days=16)

    formatted_start = format_date_for_api(start_date)
    formatted_end = format_date_for_api(end_date)

    url = (
        f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
        f"@dataInicial='{formatted_start}'&@dataFinalCotacao='{formatted_end}'&"
        f"$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json().get('value', [])
    df = pd.DataFrame(data)

    if df.empty:
        raise ValueError('No data returned from PTAX API')

    df['date'] = pd.to_datetime(df['dataHoraCotacao']).dt.date
    df.sort_values('date', inplace=True)
    df.drop_duplicates(subset='date', keep='last', inplace=True)
    df.set_index('date', inplace=True)

    return df


def fill_missing_quotes(df, end_date=None, days=10):
    '''
    Fills a date range of the last `days` calendar days ending at `end_date`
    with the most recent available PTAX quote for each day.
    If no `end_date` is provided, today is used.
    '''
    if end_date is None:
        end_date = date.today()

    target_dates = sorted([end_date - timedelta(days=i) for i in range(days)])

    filled_quotes = []
    last_known_quote = None

    for d in target_dates:
        if d in df.index:
            last_known_quote = df.loc[d]
        elif last_known_quote is None:
            previous = df[df.index < d]
            if not previous.empty:
                last_known_quote = previous.iloc[-1]
            else:
                raise ValueError(f'No available quote to fill {d}')

        filled_quotes.append({
            'date': d,
            'buy': last_known_quote['cotacaoCompra'],
            'sell': last_known_quote['cotacaoVenda'],
            'updated_at': datetime.now().date()
        })

    return pd.DataFrame(filled_quotes)


