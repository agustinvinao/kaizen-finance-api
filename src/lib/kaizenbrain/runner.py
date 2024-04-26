from typing import List
from .enums import Timeframe, Exchange
from .indicators import Indicators
from .ticks import Ticks


class Runner:
    config = None

    def __init__(self, config) -> None:
        self.config = config

    def fetch_and_insert_tick(
        self, symbol, timeframes: List[Timeframe], bars: int = 5000
    ):
        ticks = Ticks(config=self.config)
        ticks.fetch_and_insert_tick(symbol, timeframes, bars)

    def fetch_and_insert_ticks(
        self, timeframe: Timeframe, exchanges: List[Exchange], only_update=False
    ):
        ticks = Ticks(config=self.config)
        ticks.fetch_and_insert_ticks(timeframe, exchanges, only_update)

    def update_waves(self, timeframe: Timeframe):
        indicators = Indicators(config=self.config)
        indicators.update_waves(timeframe=timeframe)
