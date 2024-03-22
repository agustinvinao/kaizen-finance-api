from requests import get
from bs4 import BeautifulSoup
from datetime import timedelta
import pandas as pd
import json
from io import StringIO

from flask import Blueprint, jsonify

from helpers import (
    error_base,
    str_to_dec,
    str_to_perc,
    str_to_currency,
    date_to_str,
    next_monday,
)
from constants import api_path

module_path = "benzinga"
benzinga = Blueprint("_".join(["routes", module_path]), __name__)
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0"
}


def get_earnings_calendar():
    date_from = next_monday()
    date_to = date_from + timedelta(days=5)
    url = f"https://www.benzinga.com/calendars/earnings?date_from={date_to_str(date_from)}&date_to={date_to_str(date_to)}"
    soup = BeautifulSoup(get(url, headers=headers).content, "html.parser")
    table = soup.select_one("table.benzinga-core-virtualized-table")
    df = pd.read_html(StringIO(str(table)))[0]
    # Date▲▼ time▲▼ ticker▲▼ Quarter▲▼ Prior EPS▲▼ Est EPS▲▼ Actual EPS▲▼ EPS Surprise▲▼ Prior Rev▲▼ Est Rev▲▼ Actual Rev▲▼ Rev Surprise▲▼  Get Alert
    # 0   03/09/2024     PM      CUE        Q4      -$0.37    -$0.26            —              —    $151.00K    $2.24M            —              —  Get Alert
    # date:     mm/dd/yyy   03/09/2024
    # time:     str(2)      AM/PM
    # ticker:   str(?)      CUE
    # quater:   str(2)      Q4
    # rev_prev: $151.00K
    # rev_curr: $2.24M
    # EPS_prev: $0.58
    # EPS_est:  $0.48
    columns = {
        "Date▲▼": "date",
        "time▲▼": "time",
        "ticker▲▼": "ticker",
        "Quarter▲▼": "quarter",
        "Prior EPS▲▼": "eps_prior",
        "Est EPS▲▼": "eps_est",
        "Actual EPS▲▼": "eps_actual",
        "EPS Surprise▲▼": "eps_surprise",
        "Prior Rev▲▼": "rev_prior",
        "Est Rev▲▼": "rev_est",
        "Actual Rev▲▼": "rev_actual",
        "Rev Surprise▲▼": "rev_surprise",
        "Get Alert": "get_alert",
    }
    df.rename(columns=columns, inplace=True)
    df.drop(columns=["get_alert"])
    df["date"] = pd.to_datetime(df["date"])

    df.loc[df["eps_actual"] == "—", "eps_actual"] = ""
    df.loc[df["eps_prior"] == "—", "eps_prior"] = ""
    df.loc[df["eps_est"] == "—", "eps_est"] = ""
    df.loc[df["eps_surprise"] == "—", "eps_surprise"] = ""
    df.loc[df["rev_est"] == "—", "rev_est"] = ""
    df.loc[df["rev_actual"] == "—", "rev_actual"] = ""
    df.loc[df["rev_surprise"] == "—", "rev_surprise"] = ""
    df = df.dropna()
    df["date"] = df["date"].apply(date_to_str)

    df["eps_prior"] = df["eps_prior"].apply(str_to_dec)
    df["eps_est"] = df["eps_est"].apply(str_to_dec)
    df["eps_actual"] = df["eps_actual"].apply(str_to_dec)

    df["rev_prior"] = df["rev_prior"].apply(str_to_currency)
    df["rev_est"] = df["rev_est"].apply(str_to_currency)
    df["rev_actual"] = df["rev_actual"].apply(str_to_currency)

    df["eps_surprise"] = df["eps_surprise"].apply(str_to_perc)
    df["rev_surprise"] = df["rev_surprise"].apply(str_to_perc)

    df[
        [
            "eps_prior",
            "eps_est",
            "eps_actual",
            "rev_prior",
            "rev_est",
            "rev_actual",
            "eps_surprise",
            "rev_surprise",
        ]
    ] = df[
        [
            "eps_prior",
            "eps_est",
            "eps_actual",
            "rev_prior",
            "rev_est",
            "rev_actual",
            "eps_surprise",
            "rev_surprise",
        ]
    ].apply(
        pd.to_numeric
    )
    df.fillna("", inplace=True)
    return df


@benzinga.errorhandler(500)
def error(e):
    error_base(e)


@benzinga.errorhandler(404)
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


@benzinga.route(f"/{api_path}/{module_path}/earnings_calendar", methods=["GET"])
def earnings_calendar():
    df = get_earnings_calendar()
    return jsonify(df.to_dict(orient="records"))
