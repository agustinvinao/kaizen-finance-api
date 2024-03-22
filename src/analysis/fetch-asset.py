import yfinance as yf
from datetime import date

ticker = "ESEA"
start = "2023-01-01"
end = date.today().strftime("%Y-%m-%d")
interval = "1h"
df = yf.download(ticker, start=start, end=end, interval = interval)

df.to_csv(f"{ticker.lower()}-1h.csv")
