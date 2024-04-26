import pytz
import logging
import sys
from tvDatafeed import TvDatafeed
from datetime import timezone

from .helpers.db import (
    get_engine,
    fetch_all,
    fetch_one,
    insert_on_conflict_nothing_ticks,
)
from .helpers.sql_queries import filter_by_exchanges, query_asset_by_symbol
from .helpers.tradinview import get_interval, fetch_ticks
from .helpers.ticks import normalize_dataframe


def initLogging(config):
    logging.basicConfig(level=config["LOG_LEVEL"])
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(config["LOG_LEVEL"])
    logging.getLogger().addHandler(console)


def test(config):
    return config


def engine_from_params(config):
    return get_engine(
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        host=config["DB_HOST"],
        database=config["DB_NAME"],
    )


def fetch_and_insert_ticks(config, query_assets, timeframe, exchanges=[]):
    initLogging(config)
    tv = TvDatafeed()
    engine = engine_from_params(config)
    errors = []
    with engine.begin() as conn:
        query = query_assets.format(timeframe=timeframe)
        if len(exchanges) > 0:
            query = query + filter_by_exchanges.format(exchanges="','".join(exchanges))
        assets = fetch_all(conn, query)
        print("fetching {symbols}".format(symbols=[n["symbol"] for n in assets]))

        for asset in assets:
            tz = pytz.timezone(asset["tz"])
            bars = asset["default_bars"]
            symbol = asset["symbol"]
            exchange = asset["exchange"]
            table = f"ticks_{asset['tf']}"
            interval = get_interval(asset["tf"])

            try:
                df = fetch_ticks(tv, symbol, exchange, interval, bars)
                df[["exchange", "symbol"]] = df.symbol.str.split(":", expand=True)
                df.drop("exchange", axis=1, inplace=True)
                df = normalize_dataframe(df)
                df["dt"] = df["dt"].dt.tz_localize(tz)
                df["dt"] = df["dt"].dt.tz_convert(timezone.utc)
                print(df.head())
                df.to_sql(
                    table,
                    engine,
                    if_exists="append",
                    index=False,
                    method=insert_on_conflict_nothing_ticks,
                )
            except Exception as e:
                errors.append(asset)
                logging.error(e, exc_info=True)
        return errors


def fetch_and_insert_tick(config, symbol, timeframes=["1w"], bars=100):
    initLogging(config)
    tv = TvDatafeed()
    engine = engine_from_params(config)
    errors = []
    with engine.begin() as conn:
        asset = fetch_one(conn, query_asset_by_symbol.format(symbol))
        tz = pytz.timezone(asset["tz"])
        exchange = asset["exchange"]
        logging.info(
            "Processing {} on {} with TZ {}".format(symbol, exchange, asset["tz"])
        )
        for timeframe in timeframes:
            table = f"ticks_{timeframe}"
            interval = get_interval(timeframe)
            logging.info("Fetching interval {}".format(timeframe))
            try:
                df = fetch_ticks(tv, symbol, exchange, interval, bars)
                logging.debug("ticks count: {}".format(len(df)))
                df[["exchange", "symbol"]] = df.symbol.str.split(":", expand=True)
                df.drop("exchange", axis=1, inplace=True)
                df = normalize_dataframe(df)
                logging.debug(
                    "first row timestamp before TZ: {}".format(df["dt"].iloc[0])
                )
                df["dt"] = df["dt"].dt.tz_localize(tz, ambiguous=True)
                df["dt"] = df["dt"].dt.tz_convert(timezone.utc)
                logging.debug(
                    "first row timestamp after TZ: {}".format(df["dt"].iloc[0])
                )
                result = df.to_sql(
                    table,
                    engine,
                    if_exists="append",
                    index=False,
                    method=insert_on_conflict_nothing_ticks,
                )
                logging.debug("df to_sql result: {}".format(result))
            except Exception as e:
                errors.append(asset)
                logging.error(e, exc_info=True)

        return errors


# def lookup_and_fetch_tick(config, symbol, timeframes=['1w'], bars=100):
#   # MQTTHelper.connect()
#   fetch_and_insert_tick(config, symbol, timeframes, bars)
