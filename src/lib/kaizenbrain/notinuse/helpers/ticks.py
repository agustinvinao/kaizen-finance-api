
def fix_symbol(eachange_symbol):
  return eachange_symbol.split(':')[-1] if ':' in eachange_symbol else eachange_symbol

def normalize_dataframe(df):
  df.reset_index(inplace=True)
  df['symbol'] = df['symbol'].apply(fix_symbol)
  if 'datetime' in list(df.columns.values):
    df['dt'] = df['datetime']
    df.drop(columns=['datetime'], inplace=True)
  return df
