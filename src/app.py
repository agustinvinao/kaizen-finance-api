from flask import Flask, request, jsonify, abort
# import yfinance as yf
from datetime import date, datetime, timedelta
import json
from requests import HTTPError
from enum import Enum
# import asset.tradingview.tradingview as tradingview
# import sqlite3
# import pytz
# import finvizfinance as ff


app = Flask(__name__)

# tv = TvDatafeed()

# class KaizenInterval(Enum):
#     min1 = Interval.in_1_minute
#     min15 = Interval.in_15_minute
#     min30 = Interval.in_30_minute
#     h1 = Interval.in_1_hour
#     h4 = Interval.in_4_hour
#     d1 = Interval.in_daily
#     w1 = Interval.in_weekly
#     m1 = Interval.in_monthly


def parse_json(data):
    return json.dumps(data)

def toDate(dateString): 
    return datetime.strptime(dateString, "%Y-%m-%d").date()

def fetch_finviz_symbol(symbol):
    return ''

# def get_info(ticker):
#     try:
#         return yf.Tickers(ticker).tickers[ticker].info
#     except HTTPError as e:
#         raise e


# def add_quote_type_info(data, data_raw):
#     if data["quoteType"] == "ETF":
#         data.update({"fundFamily": data_raw["fundFamily"]})

#     elif data["quoteType"] == "EQUITY":
#         data.update(
#             {
#                 "industry": data_raw["industry"],
#                 "industryKey": data_raw["industryKey"],
#                 "industryDisp": data_raw["industryDisp"],
#                 "sector": data_raw["sector"],
#                 "sectorKey": data_raw["sectorKey"],
#             }
#         )
#     return data



# @app.route("/api/tradingview/today/<exchange>/<symbol>", methods=["GET"])
# def tradingview_today(symbol, exchange):
#     if request.method == "GET":
#         now = datetime.now()
#         midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
#         seconds = (now - midnight).seconds
#         hours = int(seconds / 3600)
        
#         df = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.in_1_hour, n_bars=hours, extended_session=True)
#         df = df.loc[midnight:].reset_index()
#         # print(df.head())
#         df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
#         return jsonify(df.to_dict(orient="records"))
    
# @app.route("/api/tradingview/history/<interval>/<exchange>/<symbol>", methods=["GET"])
# def tradingview_history(interval, exchange, symbol):
#     if request.method == "GET":
#         starts = request.args.get('starts', default = date.today(), type = toDate)
#         ends = request.args.get('ends', default = date.today() + timedelta(days=1), type = toDate)
#         n_bars = (ends - starts).days
#         if KaizenInterval[interval].value == Interval.in_4_hour:
#             n_bars = n_bars * 6
#         elif KaizenInterval[interval].value == Interval.in_1_hour:
#             n_bars = n_bars * 24
#         elif KaizenInterval[interval].value == Interval.in_30_minute:
#             n_bars = n_bars * 48
        
#         # print(f">>>>>> interval: {KaizenInterval[interval].value} starts: {starts} | ends: {ends} | n_bars: {n_bars}")
#         # print(f"@@@@ starts: {starts}")
#         # print(f"@@@@ ends: {ends}")
        
#         df = tv.get_hist(symbol=symbol, exchange=exchange, interval=KaizenInterval[interval].value, n_bars=n_bars, extended_session=True)
#         # df = tv.get_hist(symbol=symbol, exchange=exchange) #, interval=Interval.in_4_hour, n_bars=n_bars)
#         df = df.loc[starts:].reset_index()
#         df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
#         return jsonify(df.to_dict(orient="records"))

# @app.route("/api/info/<symbol>", methods=["GET"])
# def info(symbol):
#     if request.method == "GET":
#         try:
#             data_raw = get_info(symbol)
#             print(data_raw)
#             data = dict(
#                 {
#                     "currency": data_raw["currency"],
#                     "exchange": data_raw["exchange"],
#                     "gmtOffSetMilliseconds": data_raw["gmtOffSetMilliseconds"],
#                     "longName": data_raw["longName"],
#                     "quoteType": data_raw["quoteType"],
#                     "shortName": data_raw["shortName"],
#                     "timeZoneFullName": data_raw["timeZoneFullName"],
#                     "timeZoneShortName": data_raw["timeZoneShortName"],
#                     "longBusinessSummary": data_raw["longBusinessSummary"],
#                     "auditRisk": data_raw["auditRisk"],
#                     "overallRisk": data_raw["overallRisk"],
#                     "fiftyTwoWeekLow": data_raw["fiftyTwoWeekLow"],
#                     "fiftyTwoWeekHigh": data_raw["fiftyTwoWeekHigh"],
#                     "fiftyDayAverage": data_raw["fiftyDayAverage"],
#                     "recommendationKey": data_raw["recommendationKey"],
#                     "numberOfAnalystOpinions": data_raw["numberOfAnalystOpinions"]
                    
#                 }
#             )
#             return jsonify(add_quote_type_info(data, data_raw))
#         except HTTPError as e:
#             return jsonify({"message": "Symbol not found"}), e.response.status_code

# @app.route("/api/finviz/<symbol>", methods=["GET"])
# def finviz(symbol):
#     if request.method == "GET":
#         try:
#             data_raw = fetch_finviz_symbol(symbol)
#             print(data_raw)
            
#             return jsonify({"msg": "ok"})
#         except HTTPError as e:
#             return jsonify({"message": "Symbol not found"}), e.response.status_code

# @app.route("/api/quotes/<symbol>", methods=["GET"])
# def quotes_single(symbol):
#     if request.method == "GET":
#         ticker = yf.Ticker(symbol)
#         df = ticker.history(interval="1h", period="1d")
#         dublin = pytz.timezone("Europe/Dublin")
#         df.index = df.index.tz_convert(dublin)
#         df["datetime"] = df.index.to_series().apply(
#             lambda x: x.strftime("%Y-%m-%d %H:%M:%S %z")
#         )
#         data = df.reset_index()
#         return jsonify(data.to_dict(orient="records"))

# @app.route("/api/history/<symbol>", methods=["GET"])
# def history(symbol):
#     if request.method == "GET":
#         startAt = datetime(2021, 1, 1)
#         endAt = date.today() + timedelta(days=1)
#         df = yf.download(symbol, start=startAt, end=endAt)
#         print(df.head())
#         df["datetime"] = df.index.to_series().apply(
#             lambda x: x.strftime("%Y-%m-%d 00:00:00 +0000")
#         )
#         data = df.reset_index()
#         return jsonify(data.to_dict(orient="records"))


# @app.route("/api/quotes/multiple", methods=["GET"])
# def resources():
#     if request.method == "GET":
#         symbols = request.args.get("symbols")
#         print(symbols)
#         tickers = yf.Tickers(symbols)
#         df = tickers.history(
#             interval="1h", period="1mo", group_by="ticker", prepost=True, threads=True
#         )
#         df["symbol"] = data.index.to_series()
#         # dublin = pytz.timezone('Europe/Dublin')
#         # df.index = df.index.tz_convert(dublin)
#         # df['datetime'] = df.index.to_series().apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f %z"))
#         df.reset_index().melt(
#             id_vars=["index", "Datetime", "GOOG", "AAPL"], value_vars=["value"]
#         ).drop("variable", 1)
#         data = df
#         print(data)
#         print(data.info())
#         return parse_json(data.to_dict(orient="records"))


if __name__ == "__main__":
    app.debug = True
    app.run()
