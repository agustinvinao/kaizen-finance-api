from requests import HTTPError

from flask import Blueprint, jsonify, request
import yfinance as yf

from constants import api_path

module_path     = "yahoo_finance"
yahoo_finance   = Blueprint("_".join(["routes", module_path]),__name__)

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
    

@yahoo_finance.route(f"/{api_path}/{module_path}/<symbol>", methods=["GET"])
def info(symbol):
    if request.method == "GET":
        try:
            data_raw = get_info(symbol)
            print(data_raw)
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
                    "longBusinessSummary": data_raw["longBusinessSummary"],
                    "auditRisk": data_raw["auditRisk"],
                    "overallRisk": data_raw["overallRisk"],
                    "fiftyTwoWeekLow": data_raw["fiftyTwoWeekLow"],
                    "fiftyTwoWeekHigh": data_raw["fiftyTwoWeekHigh"],
                    "fiftyDayAverage": data_raw["fiftyDayAverage"],
                    "recommendationKey": data_raw["recommendationKey"],
                    "numberOfAnalystOpinions": data_raw["numberOfAnalystOpinions"]
                    
                }
            )
            return jsonify(add_quote_type_info(data, data_raw))
        except HTTPError as e:
            return jsonify({"message": "Symbol not found"}), e.response.status_code
