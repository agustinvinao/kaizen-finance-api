from datetime import date, datetime, timedelta
from enum import Enum
from helpers import to_date
from constants import api_path
import json
import traceback

from flask import Blueprint, jsonify, request
from tvDatafeed import TvDatafeed, Interval

module_path = "tradingview"
tradingview = Blueprint("_".join(["routes", module_path]),__name__)

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

@tradingview.errorhandler(500)
def error(e):
    print(traceback.format_exc())
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response
    # return jsonify(error=500, text=str(e)), 500

@tradingview.route(f"/{api_path}/{module_path}/today/<exchange>/<symbol>", methods=["GET"])
def today(symbol, exchange):
    if request.method == "GET":
        now = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds = (now - midnight).seconds
        hours = int(seconds / 3600)
        try:
            df = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.in_1_hour, n_bars=hours, extended_session=True)
            df = df.loc[midnight:].reset_index()
            df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
            return jsonify(df.to_dict(orient="records"))
        except Exception as e:
            raise e


@tradingview.route("/api/tradingview/history/<interval>/<exchange>/<symbol>", methods=["GET"])
def history(interval, exchange, symbol):
    if request.method == "GET":
        starts = request.args.get('starts', default = date.today(), type = to_date)
        ends = request.args.get('ends', default = date.today() + timedelta(days=1), type = to_date)
        n_bars = (ends - starts).days
        if KaizenInterval[interval].value == Interval.in_4_hour:
            n_bars = n_bars * 6
        elif KaizenInterval[interval].value == Interval.in_1_hour:
            n_bars = n_bars * 24
        elif KaizenInterval[interval].value == Interval.in_30_minute:
            n_bars = n_bars * 48
        df = tv.get_hist(symbol=symbol, exchange=exchange, interval=KaizenInterval[interval].value, n_bars=n_bars, extended_session=True)
        df = df.loc[starts:].reset_index()
        df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(df.to_dict(orient="records"))