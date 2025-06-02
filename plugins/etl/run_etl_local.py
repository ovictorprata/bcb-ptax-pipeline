from extract import extract_dolar_data
from transform import transform_dolar_data, generate_complete_series
from load import load_to_parquet
import pandas as pd

START_DATE = '2025-01-01'
END_DATE = '2025-01-31'

if __name__ == '__main__':
    raw_df = extract_dolar_data(START_DATE, END_DATE)
    print("Extracted data:", raw_df.head())

    transformed_df = transform_dolar_data(raw_df)
    print("Transformed data:", transformed_df.head())

    completed_df = generate_complete_series(transformed_df, START_DATE, END_DATE)

    # Filter only the actual date range after filling
    filtered_df = completed_df[
        (completed_df['date'] >= pd.to_datetime(START_DATE)) &
        (completed_df['date'] <= pd.to_datetime(END_DATE))
    ].reset_index(drop=True)

    print("Final filtered data:\n", filtered_df.head(31))
    load_to_parquet(filtered_df, 'data/base_mesa_local.parquet')
    print("Data saved to data/base_mesa_local.parquet")
