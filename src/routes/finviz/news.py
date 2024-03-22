import pandas as pd

from flask import Blueprint, jsonify, request
from finvizfinance.news import News

from helpers import error_base
from constants import api_path
from . import module_path


submodule_path = "news"
finviz = Blueprint("_".join(["routes", module_path, submodule_path]), __name__)


@finviz.errorhandler(500)
def error(e):
    error_base(e)


@finviz.route(f"/{api_path}/{module_path}/{submodule_path}")
@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/<category>", methods=["GET"])
def news(category=None):
    if request.method == "GET":
        fnews = News()
        all_news = fnews.get_news() if category == None else fnews.get_news()[category]
        return jsonify(all_news.to_dict(orient="records"))
