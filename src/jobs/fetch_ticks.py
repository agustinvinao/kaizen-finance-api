from tvDatafeed import TvDatafeed, Interval
from .helpers import get_engine, run_sql, insert_on_conflict_nothing_ticks

def fetch_ticks(tv, symbol, exchange, bars=100):
  df = tv.get_hist(
      symbol=symbol,
      exchange=exchange,
      interval=Interval.in_1_hour,
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
  # queries
  #   query_today
  query = """SELECT s.symbol as symbol, s.exchange as exchange
             FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
             WHERE p.quote_last_quote_at < CURRENT_DATE AND s.category='stock'"""
  #   query_last_hour
  query = """SELECT s.symbol, s.exchange, DATE_TRUNC('hour', now()) - quote_last_quote_at AS datediff
             FROM securities s INNER JOIN positions p ON s.symbol = p.symbol
             WHERE quote_last_quote_at < DATE_TRUNC('hour', now())"""
  # fetching securities for update
  securities = None
  with engine.begin() as conn:
    securities = run_sql(conn, query).mappings().all()
  
  for security in securities:
    symbol    = security['symbol']
    exchange  = security['exchange']
    logging.info(f"Processing {exchange}:{symbol}")
    try:
      df = fetch_ticks(tv, symbol, exchange, bars=7*24)
      logging.info(f"  count: {df.shape[0]}")
      df = normalize_dataframe(df)
      df.to_sql('ticks', engine, if_exists='append', index=False, method=insert_on_conflict_nothing_ticks)
    except Exception as e:
      logging.error(f"Exception {exchange}:{symbol}")
      logging.eroor(str(e))
  
  logging.info('fetch_ticks::finishing')