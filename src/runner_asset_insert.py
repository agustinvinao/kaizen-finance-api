import os, sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)
from lib.kaizenbrain.runner import Runner
from lib.kaizenbrain.enums import Timeframe

from dotenv import dotenv_values

path_to_env = "{}/src/lib/kaizenbrain/.env".format(project_path)
config = dotenv_values(path_to_env)

Runner(config=config).fetch_and_insert_tick(
    symbol="PYPL", timeframes=[Timeframe.W1], bars=3000
)
