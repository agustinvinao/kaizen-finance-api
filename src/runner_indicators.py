import os, sys
import time

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)
from lib.kaizenbrain.runner import Runner
from lib.kaizenbrain.enums import Timeframe

from dotenv import dotenv_values

path_to_env = "{}/src/lib/kaizenbrain/.env".format(project_path)
config = dotenv_values(path_to_env)

runner = Runner(config=config)
start_time = time.time()
runner.update_waves(timeframe=Timeframe.W1)
print("--- %s seconds ---" % (time.time() - start_time))

# https://raposa.trade/blog/higher-highs-lower-lows-and-calculating-price-trends-in-python/