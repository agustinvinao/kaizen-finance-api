# import sqlite3
from flask import Flask, request, jsonify, abort
import yfinance as yf
from datetime import date, datetime
import json
import pytz
from requests import HTTPError


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


@app.route("/api/info/<symbol>", methods=["GET"])
def info(symbol):
    if request.method == "GET":
        try:
            print(f"symbol: {symbol}")
            data_raw = get_info(symbol)
            print(f"data_raw: {data_raw}")
            data = dict(
                {
                    "currency": data_raw["currency"],
                    "exchange": data_raw["exchange"],
                    "gmtOffSetMilliseconds": data_raw["gmtOffSetMilliseconds"],
                    "longName": data_raw["longName"],
                    "quoteType": data_raw["quoteType"],  # ETF, EQUITY
                    "shortName": data_raw["shortName"],
                    "timeZoneFullName": data_raw["timeZoneFullName"],
                    "timeZoneShortName": data_raw["timeZoneShortName"],
                }
            )
            print(f"data: {data}")
            return jsonify(add_quote_type_info(data, data_raw))
        except HTTPError as e:
            return jsonify({"message": "Symbol not found"}), e.response.status_code


@app.route("/api/quotes/multiple", methods=["GET"])
def resources():
    if request.method == "GET":
        symbols = request.args.get("symbols")
        print(symbols)
        tickers = yf.Tickers(symbols)
        df = tickers.history(
            interval="1h", period="1mo", group_by="ticker", prepost=True, threads=True
        )
        df["symbol"] = data.index.to_series()
        # dublin = pytz.timezone('Europe/Dublin')
        # df.index = df.index.tz_convert(dublin)
        # df['datetime'] = df.index.to_series().apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S.%f %z"))
        df.reset_index().melt(
            id_vars=["index", "Datetime", "GOOG", "AAPL"], value_vars=["value"]
        ).drop("variable", 1)
        data = df
        print(data)
        print(data.info())
        return parse_json(data.to_dict(orient="records"))


if __name__ == "__main__":
    app.debug = True
    app.run()
