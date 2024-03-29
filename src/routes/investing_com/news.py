import pandas as pd
from datetime import date

from helpers import to_date

from flask import Blueprint, jsonify, request
import investpy

from helpers import error_base
from constants import api_path
from . import module_path

date_format = "%d/%m/%Y"
submodule_path = "news"
investing_com = Blueprint("_".join(["routes", module_path, submodule_path]), __name__)


@investing_com.errorhandler(500)
def error(e):
    error_base(e)


# @finviz.route(f"/{api_path}/{module_path}/{submodule_path}")
# @finviz.route(f"/{api_path}/{module_path}/{submodule_path}/<from_date>", methods=["GET"])
# def news(from_date=date.today):
#     if request.method == "GET":
#         fnews = news.economic_calendar(from_date=to_date(from_date, date_format))
#         return jsonify(fnews.to_dict(orient="records"))


# @finviz.route(f"/{api_path}/{module_path}/{submodule_path}")
@investing_com.route(f"/{api_path}/{module_path}/{submodule_path}", methods=["GET"])
def news():
    if request.method == "GET":
        countries = [
            "Euro Zone",
            "United States",
            "Germany",
            "Spain",
        ]  # , 'France', 'United Kingdom'
        importances = ["high", "medium"]
        fnews = investpy.news.economic_calendar(
            countries=countries, importances=importances
        )
        return jsonify(fnews.to_dict(orient="records"))
