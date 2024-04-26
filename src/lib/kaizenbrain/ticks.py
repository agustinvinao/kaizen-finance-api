import logging
import pytz
import sys
import pandas as pd
from typing import List
from datetime import timezone
from tvDatafeed import Interval, TvDatafeed
from .storage import Storage, insert_on_conflict_nothing_ticks
from .enums import Timeframe, Exchange


class Ticks:
    config = None
    storage = None
    tv = None
    errors = []

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(config["LOG_LEVEL"])
        logging.basicConfig(level=config["LOG_LEVEL"])
        if console not in logging.getLogger().handlers:
            logging.getLogger().addHandler(console)
        self.storage = Storage(config=config)
        self.tv = TvDatafeed()

    def fetch_asset(self, symbol):
        asset = self.storage.fetch_one(
            self.storage.query_asset_by_symbol.format(symbol)
        )
        return asset

    def fetch_assets(
        self, timeframe: Timeframe, exchanges: List[Exchange], only_update=False
    ):
        sql = (
            self.storage.query_assets_by_timeframe_and_exchange_without_update
            if only_update
            else self.storage.query_assets_by_timeframe_and_exchange
        )
        query = sql.format(timeframe=timeframe.value)
        if len(exchanges) > 0:
            query = query + self.storage.filter_by_exchanges.format(
                exchanges="','".join([exchange.value for exchange in exchanges])
            )
        return self.storage.fetch_all(query)

    def ticks_fetch_and_normalize(
        self, tz: str, symbol: str, exchange_str: str, interval: Interval, bars: int
    ):
        tz = pytz.timezone(tz)
        df = self.fetch_ticks(symbol, exchange_str, interval, bars)
        logging.debug("ticks count: {}".format(len(df)))
        # EXCHANGE:SYMBOL -> SYMBOL
        df[["exchange", "symbol"]] = df.symbol.str.split(":", expand=True)
        df.drop("exchange", axis=1, inplace=True)
        df = self.normalize_df(df, tz)
        return df

    def fetch_and_insert_ticks(
        self, timeframe: Timeframe, exchanges: List[Exchange] = [], only_update=False
    ):
        self.errors = []
        assets = self.fetch_assets(timeframe, exchanges, only_update)
        print("fetching {symbols}".format(symbols=[n["symbol"] for n in assets]))
        for asset in assets:
            try:
                df = self.ticks_fetch_and_normalize(
                    asset["tz"],
                    asset["symbol"],
                    asset["exchange"],
                    self.get_interval(timeframe),
                    asset["default_bars"],
                )
                df.to_sql(
                    f"ticks_{asset['tf']}",
                    self.storage.engine,
                    if_exists="append",
                    index=False,
                    method=insert_on_conflict_nothing_ticks,
                )
            except Exception as e:
                self.errors.append(asset)
                logging.error(e, exc_info=True)
        return self.errors

    def fetch_and_insert_tick(
        self, symbol: str, timeframes=[Timeframe.W1], bars: int = 100
    ):
        self.errors = []
        asset = self.fetch_asset(symbol)
        logging.info(
            "Processing {} on {} with TZ {}".format(
                symbol, asset["exchange"], asset["tz"]
            )
        )
        for timeframe in timeframes:
            logging.info("Fetching interval {}".format(timeframe))
            try:
                df = self.ticks_fetch_and_normalize(
                    asset["tz"],
                    symbol,
                    asset["exchange"],
                    self.get_interval(timeframe),
                    bars,
                )
                self.storage.save_df(
                    df,
                    table=f"ticks_{timeframe.value}",
                    method=insert_on_conflict_nothing_ticks,
                )
            except Exception as e:
                self.errors.append(asset)
                logging.error(e, exc_info=True)

    def fix_symbol(self, eachange_symbol: str):
        return (
            eachange_symbol.split(":")[-1]
            if ":" in eachange_symbol
            else eachange_symbol
        )

    def normalize_df(self, df: pd.DataFrame, tz):
        df.reset_index(inplace=True)
        df["symbol"] = df["symbol"].apply(self.fix_symbol)
        if "datetime" in list(df.columns.values):
            df["dt"] = df["datetime"]
            df.drop(columns=["datetime"], inplace=True)
        logging.debug("first row timestamp before TZ: {}".format(df["dt"].iloc[0]))
        df["dt"] = df["dt"].dt.tz_localize(tz, ambiguous=True)
        df["dt"] = df["dt"].dt.tz_convert(timezone.utc)
        logging.debug("first row timestamp after TZ: {}".format(df["dt"].iloc[0]))
        return df

    def get_interval(self, tf: Timeframe):
        if tf == Timeframe.D1:
            return Interval.in_daily
        elif tf == Timeframe.W1:
            return Interval.in_weekly
        elif tf == Timeframe.H4:
            return Interval.in_4_hour
        elif tf == Timeframe.H1:
            return Interval.in_1_hour

    def fetch_ticks(
        self,
        symbol: str,
        exchange_str: str,
        interval=Interval.in_daily,
        bars: int = 100,
        extended_session=True,
    ):
        df = self.tv.get_hist(
            symbol=symbol,
            exchange=exchange_str,
            interval=interval,
            n_bars=bars,
            extended_session=extended_session,
        )
        return df
