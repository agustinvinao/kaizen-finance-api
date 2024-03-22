from talib import abstract, get_function_groups
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import json


def pp_json(json_thing, sort=True, indents=4):
    if type(json_thing) is str:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None


def load_data(asset, timeframe):
    # Yfinance doesn't have great minute data
    df = yf.download(asset, start=start_time, end=end_time, interval=timeframe)
    df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        },
        inplace=True,
    )
    return df


if __name__ == "__main__":
    asset = "SUrG"
    timeframe = "1d"
    # timeframe = "1wk"

    end_time = datetime.now()
    start_time = end_time - timedelta(days=90)

    df = load_data(asset, timeframe)

    all_indicators = get_function_groups()["Pattern Recognition"]
    for indicator in all_indicators:
        df[str(indicator)] = getattr(abstract, indicator)(df)

    df.reset_index(inplace=True)
    df["Date"] = df["Date"].astype(str)

    json_data = dict()
    for indicator in all_indicators:
        rslt_df = df.loc[df[indicator] > 0]
        if len(rslt_df.index) > 0:
            # json_data[indicator] = [o['Date'] for o in rslt_df[['Date']].to_dict(orient='records')]
            for date in [
                o["Date"] for o in rslt_df[["Date"]].to_dict(orient="records")
            ]:
                if date not in json_data.keys():
                    json_data[date] = []
                json_data[date].append(indicator)
            # return

    print(json_data)
