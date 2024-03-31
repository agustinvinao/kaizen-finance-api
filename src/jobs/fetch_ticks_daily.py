from tvDatafeed import TvDatafeed, Interval
from helpers_db import get_engine, run_sql, postgres_upsert
from helpers_queries import queries

def fetch_ticks(tv, symbol, exchange, interval=Interval.in_daily, bars=100):
  df = tv.get_hist(
      symbol=symbol,
      exchange=exchange,
      interval=interval,
      n_bars=bars,
      extended_session=True,
  )
  return df

def fix_symbol(eachange_symbol):
  return eachange_symbol.split(':')[-1] if ':' in eachange_symbol else eachange_symbol

def normalize_dataframe(df):
  df.reset_index(inplace=True)
  df['symbol'] = df['symbol'].apply(fix_symbol)
  if 'datetime' in list(df.columns.values):
    df['dt'] = df['datetime']
    df.drop(columns=['datetime'], inplace=True)
  return df

# JOB
def job(logging):
  logging.info('fetch_ticks::starting')
  
  # init variables
  engine  = get_engine()
  tv      = TvDatafeed()
  
  with engine.begin() as conn:
    assets = run_sql(conn, queries['list_assets_today']).mappings().all()
  
    for asset in assets:
      symbol    = asset['symbol']
      exchange  = asset['exchange']
      bars      = 10
      interval  = Interval.in_daily
      logging.info(f"Processing {exchange}:{symbol}")
      logging.info(f"  count: {bars}")
      try:
        df = fetch_ticks(tv, symbol=symbol, exchange=exchange, bars=bars, interval=interval)
        df = normalize_dataframe(df)
        df.to_sql('ticks_1d', engine, if_exists='append', index=False, method=postgres_upsert)
        
      except Exception as e:
        logging.error(f"Exception {exchange}:{symbol}")
        logging.error(str(e))
  
  logging.info('fetch_ticks::finishing')