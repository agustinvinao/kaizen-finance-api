from datetime import date, datetime, timedelta
from enum import Enum
import os
import json
import traceback

from talib import abstract, get_function_groups
from flask import Blueprint, jsonify, request
from tvDatafeed import TvDatafeed, Interval

from helpers import to_date, error_base
from constants import api_path

module_path = "tradingview"
tradingview = Blueprint("_".join(["routes", module_path]), __name__)

date_format = "%Y-%m-%d"


# ['in_1_minute', 'in_3_minute', 'in_5_minute', 'in_15_minute', 'in_30_minute',
#  'in_45_minute', 'in_1_hour', 'in_2_hour', 'in_3_hour', 'in_4_hour',
#  'in_daily', 'in_weekly', 'in_monthly']
class KaizenInterval(Enum):
    min1 = Interval.in_1_minute
    min15 = Interval.in_15_minute
    min30 = Interval.in_30_minute
    h1 = Interval.in_1_hour
    h4 = Interval.in_4_hour
    d1 = Interval.in_daily
    w1 = Interval.in_weekly
    m1 = Interval.in_monthly


tv = TvDatafeed()


def get_kaizen_interval(interval):
    if KaizenInterval[interval].value == Interval.in_monthly:
        return Interval.in_monthly
    elif KaizenInterval[interval].value == Interval.in_weekly:
        return Interval.in_weekly
    elif KaizenInterval[interval].value == Interval.in_daily:
        return Interval.in_daily
    elif KaizenInterval[interval].value == Interval.in_4_hour:
        return Interval.in_4_hour
    elif KaizenInterval[interval].value == Interval.in_1_hour:
        return Interval.in_1_hour
    elif KaizenInterval[interval].value == Interval.in_30_minute:
        return Interval.in_30_minute


def starts_days_by_interval(interval):
    if get_kaizen_interval(interval) == Interval.in_4_hour:
        return 14
    elif get_kaizen_interval(interval) == Interval.in_daily:
        return 30
    elif get_kaizen_interval(interval) == Interval.in_weekly:
        return 30


def patter_recognition_details(indicator):
    base_dir = "/Users/agustinvinao/dev/shared/personal/portfolio/kaizen-finance-api/src/routes/tradingview/"
    pattern_recognition_file = "pattern_recognition.json"
    json_path = os.path.join(base_dir, pattern_recognition_file)
    with open(json_path) as pattern_recognition_details:
        pattern_recognition_details = json.load(pattern_recognition_details)
        indicator_details = pattern_recognition_details[indicator]
        return {
            "name": indicator_details["name"],
            "info": indicator_details["info"],
            "signal": indicator_details["signal"],
            "pattern": indicator_details["pattern"],
            "reliability": indicator_details["reliability"],
        }


@tradingview.errorhandler(500)
def error(e):
    error_base(e)


@tradingview.route(
    f"/{api_path}/{module_path}/today/<exchange>/<symbol>", methods=["GET"]
)
def today(symbol, exchange):
    if request.method == "GET":
        now = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds = (now - midnight).seconds
        hours = int(seconds / 3600)
        try:
            df = tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.in_1_hour,
                n_bars=hours,
                extended_session=True,
            )
            df = df.loc[midnight:].reset_index()
            df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
            return jsonify(df.to_dict(orient="records"))
        except Exception as e:
            raise e


@tradingview.route(
    f"/{api_path}/{module_path}/history/<interval>/<exchange>/<symbol>", methods=["GET"]
)
def history(interval, exchange, symbol):
    if request.method == "GET":
        today = date.today() - timedelta(days=1)
        starts = request.args.get("starts", default=date.today(), type=to_date)
        # ends = request.args.get('ends', default = date.today() + timedelta(days=1), type = to_date)
        # n_bars = (ends - starts).days
        n_bars = (today - starts).days
        if get_kaizen_interval(interval=interval) == Interval.in_1_hour:
            n_bars = 24
        if get_kaizen_interval(interval=interval) == Interval.in_4_hour:
            n_bars = n_bars * 6
        elif get_kaizen_interval(interval=interval) == Interval.in_1_hour:
            n_bars = n_bars * 24
        elif get_kaizen_interval(interval=interval) == Interval.in_30_minute:
            n_bars = n_bars * 48
        # print(f"@@@ symbol: {symbol} | exchange: {exchange} | interval: {interval} | n_bars: {n_bars}")
        df = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=get_kaizen_interval(interval),
            n_bars=abs(n_bars),
            extended_session=True,
        )
        # print(df)
        df = df.loc[starts:].reset_index()
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(df.to_dict(orient="records"))
        # return jsonify({})


@tradingview.route(
    "/api/tradingview/patterns/<interval>/<exchange>/<symbol>", methods=["GET"]
)
def patterns(interval, exchange, symbol):
    if request.method == "GET":
        df = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=get_kaizen_interval(interval),
            n_bars=50,
            extended_session=True,
        )
        df.reset_index(inplace=True)
        df["datetime"] = df["datetime"].astype(str)

        all_indicators = get_function_groups()["Pattern Recognition"]
        for indicator in all_indicators:
            df[str(indicator)] = getattr(abstract, indicator)(df)

        json_data = dict()
        for indicator in all_indicators:
            rslt_df = df.loc[df[indicator] > 0]
            if len(rslt_df.index) > 0:

                dates = [
                    o["datetime"]
                    for o in rslt_df[["datetime"]].to_dict(orient="records")
                ][::-1]
                if len(dates) > 0:
                    for date in dates:
                        if date not in json_data.keys():
                            json_data[date] = []
                        json_data[date].append(patter_recognition_details(indicator))
        return jsonify(json_data)


# https://www.tipranks.com/stocks/meta/forecast
