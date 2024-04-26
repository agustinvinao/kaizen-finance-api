Bucket_width = 1 hour

SELECT add_continuous_aggregate_policy(
  continuous_aggregate => 'one_hour_symmary',
  start_offset         => '12 hour',
  end_offset           => '1 minute',
  schedule_interval    => '1 hour')



```python
import pandas as pd
import pandas_market_calendars as mcal
from pytz import timezone

def get_forex_trading_hours(start_date, end_date):
    forex_sessions = {
        "NYSE": "XNYS",
        "ASX": "XASX",
        "JPX": "XTKS",
        "LSE": "XLON"
    }

    trading_hours = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq="D"))

    for market, exchange_code in forex_sessions.items():
        calendar = mcal.get_calendar(exchange_code)
        schedule = calendar.schedule(start_date=start_date, end_date=end_date)

        # Convert to local time zone
        local_tz = timezone("America/New_York")  # Change to your local time zone
        trading_hours[f"{market}_open"] = schedule["market_open"].dt.tz_localize("UTC").dt.tz_convert(local_tz).dt.time
        trading_hours[f"{market}_close"] = schedule["market_close"].dt.tz_localize("UTC").dt.tz_convert(local_tz).dt.time

    return trading_hours

# Example usage
start_date = "2023-11-05"
end_date = "2023-11-11"
```