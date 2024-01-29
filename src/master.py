# https://flask.palletsprojects.com/en/3.0.x/blueprints/
# from routes.finviz.calendar import finviz as finviz_calendar

from flask import Flask
from routes.tradingview import tradingview
from routes.yahoo_finance import yahoo_finance
from routes.finviz.quote import finviz as finviz_stock
from routes.finviz.group import finviz as finviz_group
from routes.finviz.news import finviz as finviz_news
from routes.finviz.insider import finviz as finviz_insider
from routes.investing_com.news import investing_com as investing_com_news
from routes.investing_com.stock import investing_com as investing_com_stock

main_app = Flask(__name__)
main_app.register_blueprint(tradingview)
main_app.register_blueprint(yahoo_finance)
main_app.register_blueprint(finviz_stock)
main_app.register_blueprint(finviz_group)
main_app.register_blueprint(finviz_news)
main_app.register_blueprint(finviz_insider)
main_app.register_blueprint(investing_com_news)
main_app.register_blueprint(investing_com_stock)

if __name__ == "__main__":
    main_app.debug = True
    main_app.run()