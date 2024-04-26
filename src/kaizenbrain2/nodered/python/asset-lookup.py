import os
import json
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

def insert_on_conflict_nothing_ticks(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(
        index_elements=["symbol","dt"]
    )
    result = conn.execute(stmt)
    return result.rowcount

class TicksCRUD:
  symbol = None
  exchange = None
  tv = None
  engine = None

  def __init__(self) -> None:
    self.symbol = None
    self.exchange = None
    self.tv = TvDatafeed()
    self.engine = self.get_engine() 

  def load_from_json(self, file):
    with open(os.path.join(os.path.split(os.path.dirname(__file__))[0], 'JsonDB', file), 'r') as f:
      return json.loads(f.read())

  def get_engine(self):
      credentials = self.load_from_json('kaizen-brain-development.json')
      return create_engine(f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}:5432/{credentials['database']}", echo=False)

  def fetch_ticks(self, tv, symbol, exchange, interval=Interval.in_daily, bars=100):
    df = tv.get_hist(
        symbol=symbol, exchange=exchange,
        interval=interval, n_bars=bars,
        extended_session=True)
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

def run_sql(con, sql):
    return con.execution_options(autocommit=True).execute(text(sql))

def get_asset(conn, symbol):
   run_sql(conn, f"""SELECT * FROM assets a WHERE symbol = {symbol}""").mappings().all()

# main
with open(os.path.join(os.path.split(os.path.dirname(__file__))[0], 'JsonDB', 'asset-lookup.json'), 'r') as f:
  contents = f.read()
  contents_json = json.loads(contents)
  symbol = contents_json["symbol"]
  exchange = contents_json["exchange"]

  tv = TvDatafeed()
  engine = get_engine()  
  with engine.begin() as conn:
    asset = get_asset(conn, symbol)
    if asset is None:
      asset = insert_asset(conn, symbol, exchange)

    for bucket in [
       {"timeframe": "4h", "interval": Interval.in_4_hour, "bars": int(3*365*4)},
       {"timeframe": "1d", "interval": Interval.in_daily, "bars": int(3*365)},
       {"timeframe": "1w", "interval": Interval.in_weekly, "bars": int(3*365/7)}
    ]:
      bars = bucket['bars']
      symbol = asset['symbol']
      exchange = asset['exchange']
      interval = bucket['interval']
      table = f"ticks_{bucket['timeframe']}"

      try:
        df = fetch_ticks(tv, symbol, exchange, interval, bars)
        df[['exchange','symbol']] = df.symbol.str.split(':', expand=True)
        df.drop('exchange', axis=1, inplace=True)
        df = normalize_dataframe(df)
        df.to_sql(table, engine, if_exists='append', index=False, method=insert_on_conflict_nothing_ticks)
      except Exception as e:
        print(e)