from ..runner import Runner

from .helpers.insider_trading import InsiderTrading
from .fetch_ticks import fetch_and_insert_ticks, fetch_and_insert_tick
from .helpers.sql_queries import (
    query_assets_by_timeframe_and_exchange,
    query_assets_by_timeframe_and_exchange_without_update,
)


def joke():
    return (
        "Wenn ist das Nunst\u00fcck git und Slotermeyer? Ja! ... "
        "Beiherhund das Oder die Flipperwaldt gersput."
    )


# ticks
def fetch_ticks(config, timeframe, exchanges=[]):
    return fetch_and_insert_ticks(
        config, query_assets_by_timeframe_and_exchange, timeframe, exchanges
    )


def fetch_ticks_not_updated(config, timeframe, exchanges=[]):
    return fetch_and_insert_ticks(
        config,
        query_assets_by_timeframe_and_exchange_without_update,
        timeframe,
        exchanges,
    )


def fetch_asset_ticks(config, symbol, timeframes=["1w"], bars=100):
    return fetch_and_insert_tick(config, symbol, timeframes, bars)


# insider_tradings
def update_insider_trading(config, debug=False):
    if debug:
        print(config)
    it = InsiderTrading(config, debug=debug)
    it.run()
