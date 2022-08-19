from email import utils
import pandas as pd
import datetime as dt

from oanda_instrument import Instrument
import oanda_utils
from oanda_api import OandaAPI

INCREMENTS = {
    'M5' : 5,
    'M15' : 15,
    'M30' : 30,
    'H1' : 60,
    'H4' : 240,
    'D' : 1440,
    # 'W' : 10080,
    # 'W' : 524160
}

def create_file(pair, granularity, api):
    candle_count = 2000
    time_step = INCREMENTS[granularity] * candle_count

    end_date = oanda_utils.get_utc_dt_from_string("2020-12-31 23:59:59")
    date_from = oanda_utils.get_utc_dt_from_string("2018-01-01 00:00:00")

    candle_dfs = []

    date_to = date_from
    while date_to < end_date:
        date_to = date_from + dt.timedelta(minutes=time_step)
        if date_to > end_date:
            date_to = end_date
        
        code, df = api.fetch_candles(pair,
                granularity=granularity,
                date_from=date_from,
                date_to=date_to,
                as_df=True)
        if df is not None and df.empty == False:
            candle_dfs.append(df)
        elif code != 200:
                print("HTTP ERROR", pair, granularity, date_from, date_to)
                break
        date_from = date_to

    final_df = pd.concat(candle_dfs)
    final_df.drop_duplicates(subset='time', inplace=True)
    final_df.sort_values(by='time', inplace=True)
    final_df.to_pickle(oanda_utils.get_his_data_filename(pair, granularity))
    print(f"{pair} {granularity} {final_df.iloc[0].time} {final_df.iloc[-1].time}")


def run_collection():
    pair_list = "GBP,EUR,USD,CAD,JPY,NZD,CHF"
    api = OandaAPI()
    for g in INCREMENTS.keys():
        for i in Instrument.get_pairs_from_string(pair_list):
            print(g, i)
            create_file(i, g, api)

if __name__ == "__main__":
    run_collection()