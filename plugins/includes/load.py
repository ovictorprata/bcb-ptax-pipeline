import pandas as pd

def load_to_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path, index=False)
