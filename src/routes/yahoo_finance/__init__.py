from requests import get
# HTTPError, 
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import json
# import traceback
# import re

from flask import Blueprint, jsonify, request
import yfinance as yf

from helpers import error_base
from constants import api_path

module_path = "yahoo_finance"
yahoo_finance = Blueprint("_".join(["routes", module_path]), __name__)
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0"
}

# TODO: hay que tomar el token para usar un http request
# def get_rartings(symbol):
#     date_to = datetime.now()
#     date_to_str = date_to.strftime('%Y-%m-%d')
#     date_from_str = (date_to + timedelta(-date_to.weekday(), weeks=-1)).strftime('%Y-%m-%d')
#     url = f'https://www.benzinga.com/analyst-stock-ratings?date_from={date_from_str}&date_to={date_to_str}&tickers={symbol}'
#     soup = BeautifulSoup(get(url, headers=headers).content, 'html.parser')
#     table = soup.select_one('table.benzinga-core-virtualized-table')
#     return pd.read_html(str(table))[0]

# IN PROGRESS
# def convert_to_float(str):
#     value = re.findall(r'\d+\.\d+', str)
#     if len(value) > 0:
#         value = float(value[0])
#     return value * -1 if str[0] == '-' else value

# # df['Prior EPS▲▼'].apply(convert_to_float)
# # df['Est EPS▲▼'].apply(convert_to_float)

# def strnum_to_value(str):
#     if str == "—":
#         return 0
#     value = extract_float(str)
#     if len(value) > 0:
#         value = float(value[0])
#         unit = str[-1]
#         multiplier = 1
#         if unit == 'K':
#             multiplier = 1000
#         elif unit == 'M':
#             multiplier = 1000000
#         elif unit == 'B':
#             multiplier = 1000000000000
#         return f"{value * multiplier}"

# # df['Prior Rev▲▼'].apply(strnum_to_value)
# # df['Est Rev▲▼'].apply(strnum_to_value)


def get_analysis_table(symbol, table):
    url = f"https://finance.yahoo.com/quote/{symbol}/analysis?p={symbol}"
    soup = BeautifulSoup(get(url, headers=headers).content, "html.parser")
    table = soup.select_one(f'table:-soup-contains("{table}")')
    return pd.read_html(str(table))[0]


def get_info(ticker):
    # try:
    # return yf.Tickers(ticker).tickers[ticker].info
    return yf.Ticker(ticker).info


# except HTTPError as e:
#     raise e


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


@yahoo_finance.errorhandler(500)
def error(e):
    error_base(e)


@yahoo_finance.errorhandler(404)
def error(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps(
        {
            "code": e.code,
            "name": e.name,
            "description": e.description,
        }
    )
    response.content_type = "application/json"
    return response


@yahoo_finance.route(f"/{api_path}/{module_path}/info/<symbol>", methods=["GET"])
def info(symbol):
    if request.method == "GET":
        # try:
        data_raw = get_info(symbol)
        print(data_raw)
        data = dict(
            {
                "auditRisk": data_raw["auditRisk"] if "auditRisk" in data_raw else "",
                "currency": data_raw["currency"],
                "exchange": data_raw["exchange"],
                "fiftyTwoWeekLow": data_raw["fiftyTwoWeekLow"],
                "fiftyTwoWeekHigh": data_raw["fiftyTwoWeekHigh"],
                "fiftyDayAverage": data_raw["fiftyDayAverage"],
                "financialCurrency": data_raw["financialCurrency"],
                "freeCashflow": (
                    data_raw["freeCashflow"] if "freeCashflow" in data_raw else ""
                ),
                "gmtOffSetMilliseconds": data_raw["gmtOffSetMilliseconds"],
                "overallRisk": (
                    data_raw["overallRisk"] if "overallRisk" in data_raw else ""
                ),
                "quoteType": data_raw["quoteType"],
                "longName": data_raw["longName"],
                "marketCap": data_raw["marketCap"],
                "numberOfAnalystOpinions": (
                    data_raw["numberOfAnalystOpinions"]
                    if "numberOfAnalystOpinions" in data_raw
                    else ""
                ),
                "shortName": data_raw["shortName"],
                "targetMedianPrice": (
                    data_raw["targetMedianPrice"]
                    if "targetMedianPrice" in data_raw
                    else ""
                ),
                "timeZoneFullName": data_raw["timeZoneFullName"],
                "timeZoneShortName": data_raw["timeZoneShortName"],
                "totalCash": data_raw["totalCash"],
                "totalDebt": data_raw["totalDebt"],
                "totalRevenue": data_raw["totalRevenue"] if "totalRevenue" in data_raw else "",
                "trailingPE": (
                    data_raw["trailingPE"] if "trailingPE" in data_raw else ""
                ),
                "forwardPE": data_raw["forwardPE"],
                "longBusinessSummary": (
                    data_raw["longBusinessSummary"]
                    if "longBusinessSummary" in data_raw
                    else ""
                ),
                "recommendationKey": (
                    data_raw["recommendationKey"]
                    if "recommendationKey" in data_raw
                    else ""
                ),
            }
        )
        return jsonify(add_quote_type_info(data, data_raw))
    # except HTTPError as e:
    #     return jsonify({"message": "Symbol not found"}), e.response.status_code


@yahoo_finance.route(f"/{api_path}/{module_path}/growth/<symbol>", methods=["GET"])
def growth(symbol):
    if request.method == "GET":
        df = get_analysis_table(symbol=symbol, table="Growth Estimates")
        return jsonify(df.to_dict(orient="records"))


@yahoo_finance.route(
    f"/{api_path}/{module_path}/earning_history/<symbol>", methods=["GET"]
)
def earning_history(symbol):
    if request.method == "GET":
        df = get_analysis_table(symbol=symbol, table="Earnings History")
        return jsonify(df.to_dict(orient="records"))


@yahoo_finance.route(
    f"/{api_path}/{module_path}/earnings_results/<symbol>", methods=["GET"]
)
def earnings_results(symbol):
    if request.method == "GET":
        df_growth = get_analysis_table(symbol=symbol, table="Growth Estimates")
        df_earnings = get_analysis_table(symbol=symbol, table="Earnings History")
        info = get_info(symbol)
        result = dict(
            {
                "symbol": info["symbol"],
                "longName": info["longName"],
                "currentPrice": info["currentPrice"],
                "companyDetails": {
                    "shortName": info["shortName"],
                    "industry": info["industry"],
                    "industryKey": info["industryKey"],
                    "sector": info["sector"],
                    "sectorKey": info["sectorKey"],
                    "longBusinessSummary": info["longBusinessSummary"],
                    "fullTimeEmployees": (
                        info["fullTimeEmployees"] if "fullTimeEmployees" in info else ""
                    ),
                },
                "overallRisk": info["overallRisk"] if "overallRisk" in info else "",
                "riskDeatails": {
                    "auditRisk": info["auditRisk"] if "auditRisk" in info else "",
                    "boardRisk": info["boardRisk"] if "boardRisk" in info else "",
                    "compensationRisk": (
                        info["compensationRisk"] if "compensationRisk" in info else ""
                    ),
                    "shareHolderRightsRisk": (
                        info["shareHolderRightsRisk"]
                        if "shareHolderRightsRisk" in info
                        else ""
                    ),
                },
                "PE": {
                    "trailingPE": info["trailingPE"] if "trailingPE" in info else "",
                    "forwardPE": info["forwardPE"],
                },
                "recommendation": {
                    "recommendationMean": info["recommendationMean"],
                    "recommendationKey": (
                        info["recommendationKey"] if "recommendationKey" in info else ""
                    ),
                    "numberOfAnalystOpinions": (
                        info["numberOfAnalystOpinions"]
                        if "numberOfAnalystOpinions" in info
                        else ""
                    ),
                },
                "target": {
                    "targetHighPrice": info["targetHighPrice"],
                    "targetLowPrice": info["targetLowPrice"],
                    "targetMeanPrice": info["targetMeanPrice"],
                    "targetMedianPrice": info["targetMedianPrice"],
                },
                "cash": {
                    "totalCash": info["totalCash"],
                    "totalCashPerShare": info["totalCashPerShare"],
                    "ebitda": info["ebitda"] if "ebitda" in info else "",
                    "totalDebt": info["totalDebt"] if "totalDebt" in info else "",
                    "quickRatio": info["quickRatio"] if "quickRatio" in info else "",
                    "currentRatio": (
                        info["currentRatio"] if "currentRatio" in info else ""
                    ),
                    "totalRevenue": (
                        info["totalRevenue"] if "totalRevenue" in info else ""
                    ),
                    "debtToEquity": (
                        info["debtToEquity"] if "debtToEquity" in info else ""
                    ),
                    "revenuePerShare": (
                        info["revenuePerShare"] if "revenuePerShare" in info else ""
                    ),
                    "returnOnAssets": (
                        info["returnOnAssets"] if "returnOnAssets" in info else ""
                    ),
                    "returnOnEquity": (
                        info["returnOnEquity"] if "returnOnEquity" in info else ""
                    ),
                    "freeCashflow": (
                        info["freeCashflow"] if "freeCashflow" in info else ""
                    ),
                    "operatingCashflow": (
                        info["operatingCashflow"] if "operatingCashflow" in info else ""
                    ),
                },
                "growth": df_growth.fillna("").to_dict(orient="records"),
                "earning_history": df_earnings.fillna("").to_dict(orient="records"),
            }
        )

        return jsonify(result)
