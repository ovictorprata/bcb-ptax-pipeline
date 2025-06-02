import requests
import pandas as pd
from datetime import datetime, timedelta

def adjust_start_date(start_date: str) -> str:
    """Subtrai 7 dias da data inicial fornecida."""
    date = datetime.strptime(start_date, "%Y-%m-%d")
    adjusted = date - timedelta(days=7)
    return adjusted.strftime("%m-%d-%Y")

def extract_dolar_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Extrai dados de cotação do dólar da API do Banco Central."""
    adjusted_start = adjust_start_date(start_date)
    formatted_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%m-%d-%Y")
    
    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
        f"@dataInicial='{adjusted_start}'&@dataFinalCotacao='{formatted_end}'&"
        "$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
    )

    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return pd.DataFrame(response.json()["value"])
