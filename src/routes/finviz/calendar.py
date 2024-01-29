import pandas as pd

from flask import Blueprint, jsonify, request
from finvizfinance.calendar import Calendar

from constants import api_path
from . import module_path


submodule_path = "calendar"
finviz  = Blueprint("_".join(["routes", module_path, submodule_path]),__name__)

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}", methods=["GET"])
def calendar():
    if request.method == "GET":
        calendar = Calendar().calendar()
        return jsonify(calendar.to_dict(orient="records"))
