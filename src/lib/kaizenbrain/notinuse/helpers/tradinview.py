from tvDatafeed import Interval

def get_interval(tf):
  if tf == '1d':
    return Interval.in_daily
  elif tf == '1w':
    return Interval.in_weekly
  elif tf == '4h':
    return Interval.in_4_hour
  elif tf == '1h':
    return Interval.in_1_hour
  
def fetch_ticks(tv, symbol, exchange, interval=Interval.in_daily, bars=100, extended_session=True):
  df = tv.get_hist(
      symbol=symbol, exchange=exchange,
      interval=interval, n_bars=bars,
      extended_session=extended_session)
  return df