# import sqlite3
from flask import Flask, request, jsonify, abort
import yfinance as yf
from datetime import date, datetime, timedelta
import json
import pytz
from requests import HTTPError
from tvDatafeed import TvDatafeed, Interval
# ['in_1_minute',
# 'in_3_minute',
# 'in_5_minute',
# 'in_15_minute',
# 'in_30_minute',
# 'in_45_minute',
# 'in_1_hour',
# 'in_2_hour',
# 'in_3_hour',
# 'in_4_hour',
# 'in_daily',
# 'in_weekly',
# 'in_monthly']
# username = 'agustinvinao'
# password = 'Chester848'
tv = TvDatafeed()
app = Flask(__name__)

def parse_json(data):
    return json.dumps(data)


def get_info(ticker):
    try:
        return yf.Tickers(ticker).tickers[ticker].info
    except HTTPError as e:
        raise e


def add_quote_type_info(data, data_raw):
    if data["quoteType"] == "ETF":
        data.update({"fundFamily": data_raw["fundFamily"]})

    elif data["quoteType"] == "EQUITY":
        data.update(
            {
                "industry": data_raw["industry"],
                "industryKey": data_raw["industryKey"],
                "industryDisp": data_raw["industryDisp"],
                "sector": data_raw["sector"],
                "sectorKey": data_raw["sectorKey"],
            }
        )
    return data


@app.route("/api/tradingview/today/<exchange>/<symbol>", methods=["GET"])
def tradingview_today(symbol, exchange):
    if request.method == "GET":
        print(f"symbol: {symbol} exchange: {exchange}")
        df = tv.get_hist(symbol=symbol,exchange=exchange,interval=Interval.in_1_hour,n_bars=100)
        df = df.reset_index()
        df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(df.to_dict(orient="records"))
    
@app.route("/api/tradingview/history/<exchange>/<symbol>", methods=["GET"])
def tradingview_history(symbol, exchange):
    if request.method == "GET":
        print(f"symbol: {symbol} exchange: {exchange}")
        df = tv.get_hist(symbol=symbol,exchange=exchange,interval=Interval.in_daily,n_bars=1000)
        df = df.reset_index()
        df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(df.to_dict(orient="records"))


@app.route("/api/quotes/<symbol>", methods=["GET"])
def quotes_single(symbol):
    if request.method == "GET":
        ticker = yf.Ticker(symbol)
        df = ticker.history(interval="1h", period="1d")
        dublin = pytz.timezone("Europe/Dublin")
        df.index = df.index.tz_convert(dublin)
        df["datetime"] = df.index.to_series().apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M:%S %z")
        )
        data = df.reset_index()
        return jsonify(data.to_dict(orient="records"))

@app.route("/api/history/<symbol>", methods=["GET"])
def history(symbol):
    if request.method == "GET":
        # ticker = yf.Ticker(symbol)
        startAt = datetime(2021, 1, 1)
        endAt = date.today() + timedelta(days=1)
        df = yf.download(symbol, start=startAt, end=endAt)
        # dublin = pytz.timezone("Europe/Dublin")
        # df.index = df.index.tz_convert(dublin)
        print(df.head())
        df["datetime"] = df.index.to_series().apply(
            lambda x: x.strftime("%Y-%m-%d 00:00:00 +0000")
        )
        data = df.reset_index()
        return jsonify(data.to_dict(orient="records"))

@app.route("/api/info/<symbol>", methods=["GET"])
def info(symbol):
    if request.method == "GET":
        try:
            data_raw = get_info(symbol)
            data = dict(
                {
                    "currency": data_raw["currency"],
                    "exchange": data_raw["exchange"],
                    "gmtOffSetMilliseconds": data_raw["gmtOffSetMilliseconds"],
                    "longName": data_raw["longName"],
                    "quoteType": data_raw["quoteType"],
                    "shortName": data_raw["shortName"],
                    "timeZoneFullName": data_raw["timeZoneFullName"],
                    "timeZoneShortName": data_raw["timeZoneShortName"],
                }
            )
            print(f"data: {data}")
            return jsonify(add_quote_type_info(data, data_raw))
        except HTTPError as e:
            return jsonify({"message": "Symbol not found"}), e.response.status_code


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
