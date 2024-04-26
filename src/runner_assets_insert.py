import os, sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)
from lib.kaizenbrain.runner import Runner
from lib.kaizenbrain.enums import Timeframe, Exchange

from dotenv import dotenv_values

path_to_env = "{}/src/lib/kaizenbrain/.env".format(project_path)
config = dotenv_values(path_to_env)

runner = Runner(config=config)
runner.fetch_and_insert_ticks(
    timeframe=Timeframe.W1,
    exchanges=[Exchange.NYSE, Exchange.NASDAQ, Exchange.XETR, Exchange.EURONEXT],
    only_update=True,
)
