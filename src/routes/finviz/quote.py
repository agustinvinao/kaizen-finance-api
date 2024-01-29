import pandas as pd

from flask import Blueprint, jsonify, request
from finvizfinance.quote import finvizfinance

from constants import api_path
from . import module_path


submodule_path = "stock"
finviz = Blueprint("_".join(["routes", module_path, submodule_path]),__name__)

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/ticker_description/<symbol>", methods=["GET"])
def ticker_description(symbol):
    if request.method == "GET":
        stock = finvizfinance(symbol)
        return jsonify({"description": stock.ticker_description()})

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/ticker_fundament/<symbol>", methods=["GET"])
def ticker_fundament(symbol):
    if request.method == "GET":
        stock = finvizfinance(symbol)
        return stock.ticker_fundament()

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/ticker_outer_ratings/<symbol>", methods=["GET"])
def ticker_outer_ratings(symbol):
    if request.method == "GET":
        stock = finvizfinance(symbol)
        ticker_outer_ratings = stock.ticker_outer_ratings()
        ticker_outer_ratings['Date'] = ticker_outer_ratings['Date'].dt.strftime("%Y-%m-%d %H:%M:%S")
        return jsonify(ticker_outer_ratings.to_dict(orient="records"))

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/ticker_inside_trader/<symbol>", methods=["GET"])
def ticker_inside_trader(symbol):
    if request.method == "GET":
        stock = finvizfinance(symbol)
        return jsonify(stock.ticker_inside_trader().to_dict(orient="records"))

@finviz.route(f"/{api_path}/{module_path}/{submodule_path}/ticker_news/<symbol>", methods=["GET"])
def ticker_news(symbol):
    if request.method == "GET":
        stock = finvizfinance(symbol)
        return jsonify(stock.ticker_news().to_dict(orient="records"))
