from enum import Enum

class Timeframe(Enum):
  H1 = "1h"
  H4 = "4h"
  D1 = "1d"
  W1 = "1w"
  M1 = "1m"

class Exchange(Enum):
  NYSE = "NYSE"
  NASDAQ = "NASDAQ"
  XETR = "XETR"
  EURONEXT = "EURONEXT"
