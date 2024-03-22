import pandas as pd
from datetime import date

from helpers import to_date

from flask import Blueprint, jsonify, request
import investpy

from helpers import error_base
from constants import api_path
from . import module_path

date_format = "%d/%m/%Y"
submodule_path = "stock"
investing_com = Blueprint("_".join(["routes", module_path, submodule_path]), __name__)


@investing_com.errorhandler(500)
def error(e):
    error_base(e)


@investing_com.route(
    f"/{api_path}/{module_path}/{submodule_path}/<country>/<symbol>", methods=["GET"]
)
def stock(country, symbol):
    if request.method == "GET":
        stock_information = investpy.stocks.get_stock_information(
            stock=symbol, country=country
        )
        return jsonify(stock_information.to_dict(orient="records"))
