import pandas as pd

from flask import Blueprint, jsonify, request
from finvizfinance.insider import Insider

from constants import api_path
from . import module_path


submodule_path = "insider"
finviz = Blueprint("_".join(["routes", module_path, submodule_path]),__name__)

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/data")
@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/data/<filter>", methods=["GET"])
def insider_data(filter=None):
    if request.method == "GET":
        finsider = Insider()
        insider = finsider.get_insider() if filter == None else finsider.get_insider(filter)
        return jsonify(insider.to_dict(orient="records"))

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/filters", methods=["GET"])
def insider_filters():
    if request.method == "GET":
        return jsonify(['latest', 'latest buys', 'latest sales', 'top week',
                        'top week buys', 'top week sales', 'top owner trade',
                        'top owner buys', 'top owner sales', 'insider_id'])

