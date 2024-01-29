import pandas as pd

from flask import Blueprint, jsonify, request
from finvizfinance.group.overview import Overview
from finvizfinance.group.performance import Performance

from constants import api_path
from . import module_path


submodule_path = "group"
finviz = Blueprint("_".join(["routes", module_path, submodule_path]),__name__)

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/screener_view/overview")
@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/screener_view/overview/<sector>", methods=["GET"])
def screener_view_overview(sector=None):
    if request.method == "GET":
        fgoverview = Overview()
        df = fgoverview.screener_view() if sector == None else fgoverview.screener_view(sector)
        return jsonify(df.to_dict(orient="records"))

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/screener_view/performance")
@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/screener_view/performance/<sector>", methods=["GET"])
def screener_view_performance(sector=None):
    if request.method == "GET":
        fgperformance = Performance()
        df = fgperformance.screener_view() if sector == None else fgperformance.screener_view(sector)
        return jsonify(df.to_dict(orient="records"))
