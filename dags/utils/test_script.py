from ptax_utils import fetch_ptax_data, fill_missing_quotes
from datetime import date, timedelta

end = date(2025, 4, 25)

print('🔄 Fetching PTAX data...')
df = fetch_ptax_data(end)
print(df)

print('\n📊 Filling last 10 days of quotes...')
filled_df = fill_missing_quotes(df, end)
print(filled_df)
