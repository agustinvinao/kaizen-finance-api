import traceback
import json
from datetime import datetime, timedelta
from re import sub
from decimal import Decimal


def to_date(date_string, format="%Y-%m-%d"):
    return datetime.strptime(date_string, format).date()


def date_to_str(date):
    return date.strftime("%Y-%m-%d")


def error_base(e):
    print(traceback.format_exc())
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


def str_to_dec(val: str):
    if len(val) == 0:
        return
    mutiplier = -1 if val.startswith("-") else 1
    return Decimal(sub(r"[^\d.]", "", val)) * mutiplier


def str_to_perc(val: str):
    if len(val) == 0:
        return
    return str_to_dec(val[:-1])


def str_to_currency(val: str):
    if len(val) == 0:
        return
    unit = val[-1]
    num = val[:-1] if unit != "0" else val
    num = str_to_dec(num)
    if unit == "0":
        return num
    elif unit == "K":
        return num * 1000
    elif unit == "M":
        return num * 1000000
    elif unit == "B":
        return num * 1000000000000


def next_monday():
    current = datetime.now()
    return current + timedelta(days=(7 - current.weekday()))
