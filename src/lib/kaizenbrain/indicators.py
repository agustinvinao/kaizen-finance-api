import logging
import sys
import pandas_ta as ta
import pandas as pd
import numpy as np
import time
from collections import deque
from typing import List
from sqlalchemy import text
from .storage import (
    Storage,
    insert_on_conflict_update_waves,
    insert_on_conflict_update_ticks,
    postgres_upsert,
)
from .enums import Timeframe

pd.options.mode.copy_on_write = True


SMA_FAST = 8
SMA_SLOW = 16
ATR_WINDOW = 11
PIVOT = 11
COL_HIGH = "high"
COL_LOW = "low"


class Indicators:
    config = None
    storage = None

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(config["LOG_LEVEL"])
        logging.basicConfig(level=config["LOG_LEVEL"])
        if console not in logging.getLogger().handlers:
            logging.getLogger().addHandler(console)
        self.storage = Storage(config=config)

    def update_waves(self, timeframe: Timeframe, pivot: int = PIVOT):
        # ticks = self.storage.fetch_all(self.storage.query_ticks.format(timeframe.value))
        # ticks = self.storage.fetch_all("SELECT * FROM ticks_{} where symbol in ('PYPL','T','AAOI')".format(timeframe.value))
        ticks = self.storage.fetch_all(
            "SELECT symbol FROM assets WHERE symbol in ('PYPL','T','AAOI')"
        )
        df = pd.DataFrame(ticks)
        # df_final = pd.DataFrame()
        symbols = df["symbol"].unique()
        # with self.storage.engine.begin() as conn:
        for symbol in symbols:
            start_time = time.time()
            ticks = self.storage.fetch_all(
                "SELECT * FROM ticks_{timeframe} where symbol = '{symbol}'".format(
                    timeframe=timeframe.value, symbol=symbol
                )
            )
            df_asset = pd.DataFrame(ticks)
            logging.info(":: processing {}...".format(symbol))
            # df_asset = df[df["symbol"] == symbol].copy(True)
            df_asset.set_index("dt", inplace=True)
            logging.info(":: adding pivots H L...")
            (
                df_asset["pivot_high"],
                df_asset["pivot_high_value"],
                df_asset["pivot_low"],
                df_asset["pivot_low_value"],
            ) = self.pivot_points(data=df_asset, pivot=pivot)

            df_asset.drop(
                columns=[
                    "close",
                    "open",
                    "high",
                    "low",
                    "volume",
                    "PH",
                    "PHV",
                    "PL",
                    "PLV",
                ],
                inplace=True,
            )
            df_asset.reset_index(inplace=True)
            # df_final = pd.concat([df_final, df_asset])
            df_asset.to_sql(
                f"ticks_waves_{timeframe.value}",
                self.storage.engine,
                if_exists="append",
                index=False,
                method=postgres_upsert
            )
            logging.info(
                ":: {} - {} seconds ...".format(symbol, (time.time() - start_time))
            )
            # df_final.to_sql(
            #     f"pivots_waves_{timeframe.value}",
            #     self.storage.engine,
            #     if_exists="replace",
            #     index=False,
            # )

    def clean_deque(self, i, k, deq, df, key, is_high):
        if deq and deq[0] == i - k:
            deq.popleft()
        if is_high:
            while deq and df.iloc[i][key] > df.iloc[deq[-1]][key]:
                deq.pop()
        else:
            while deq and df.iloc[i][key] < df.iloc[deq[-1]][key]:
                deq.pop()

    def pivot_points(self, pivot=None, data=None):
        data["PH"] = False
        data["PHV"] = np.NaN
        data["PL"] = False
        data["PLV"] = np.NaN
        key_high = "high"
        key_low = "low"
        win_size = pivot * 2 + 1
        deq_high = deque()
        deq_low = deque()
        max_idx = 0
        min_idx = 0
        i = 0
        j = pivot
        pivot_low = None
        pivot_high = None
        for index, row in data.iterrows():
            if i < win_size:
                self.clean_deque(i, win_size, deq_high, data, key_high, True)
                self.clean_deque(i, win_size, deq_low, data, key_low, False)
                deq_high.append(i)
                deq_low.append(i)
                if data.iloc[i][key_high] > data.iloc[max_idx][key_high]:
                    max_idx = i
                if data.iloc[i][key_low] < data.iloc[min_idx][key_low]:
                    min_idx = i
                if i == win_size - 1:
                    if data.iloc[max_idx][key_high] == data.iloc[j][key_high]:
                        data.at[data.index[j], "PH"] = True
                        pivot_high = data.iloc[j][key_high]
                    if data.iloc[min_idx][key_low] == data.iloc[j][key_low]:
                        data.at[data.index[j], "PL"] = True
                        pivot_low = data.iloc[j][key_low]
            if i >= win_size:
                j += 1
                self.clean_deque(i, win_size, deq_high, data, key_high, True)
                self.clean_deque(i, win_size, deq_low, data, key_low, False)
                deq_high.append(i)
                deq_low.append(i)
                pivot_val = data.iloc[deq_high[0]][key_high]
                if pivot_val == data.iloc[j][key_high]:
                    data.at[data.index[j], "PH"] = True
                    pivot_high = data.iloc[j][key_high]
                if data.iloc[deq_low[0]][key_low] == data.iloc[j][key_low]:
                    data.at[data.index[j], "PL"] = True
                    pivot_low = data.iloc[j][key_low]

            data.at[data.index[j], "PHV"] = pivot_high
            data.at[data.index[j], "PLV"] = pivot_low
            i = i + 1

        return data["PH"], data["PHV"], data["PL"], data["PLV"]
