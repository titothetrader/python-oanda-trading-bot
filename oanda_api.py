from datetime import date
import requests
import pandas as pd
from dateutil.parser import *

import defs
import oanda_utils

class OandaAPI():
    
    def __init__(self):
        self.session = requests.Session()
        
    def fetch_instruments(self):
        url = f"{defs.OANDA_URL}/accounts/{defs.ACCOUNT_ID}/instruments"
        response = self.session.get(url, params=None, headers=defs.SECURE_HEADER)
        return response.status_code, response.json()

    def fetch_instruments_df(self):
        code, data = self.fetch_instruments()
        if code == 200:
            ins_df = pd.DataFrame.from_dict(data['instruments'])
            return ins_df[['name', 'type', 'displayName', 'pipLocation', 'marginRate']]
        else:
            return print("HTTP Error")

    def save_instruments(self):
        ins_df = self.fetch_instruments_df()
        if ins_df is not None:
            ins_df.to_pickle(oanda_utils.get_instruments_data_filename())

    def fetch_candles(self, pair_name, count=None, granularity="H1", date_from=None, date_to=None, as_df=False):
        url = f"{defs.OANDA_URL}/instruments/{pair_name}/candles"
        params = dict(
            granularity = granularity,
            price = "MBA"
        )

        if date_from is not None and date_to is not None:
            params['to'] = int(date_to.timestamp())
            params['from'] = int(date_from.timestamp())
        elif count is not None:
            params['count'] = count
        else:
            params['count'] = 300

        response = self.session.get(url, params=params, headers=defs.SECURE_HEADER)

        if response.status_code !=200:
            return response.status_code, None

        if as_df == True: 
            json_data = response.json()['candles']
            return response.status_code, OandaAPI.candles_to_df(json_data)
        else:
            return response.status_code, response.json()

    def fetch_candles_df(self, json_response):
        prices = ['mid', 'bid', 'ask']
        ohlc = ['o', 'h', 'l', 'c']
        our_data = []
        for candle in json_response['candles']:
            if candle['complete'] == False:
                continue
            new_dict = {}
            new_dict['time'] = candle['time']
            new_dict['volume'] = candle['volume']
            for price in prices:
                for oh in ohlc:
                    new_dict[f"{price}_{oh}"] = candle[price][oh]
                our_data.append(new_dict)
        return pd.DataFrame.from_dict(our_data)

    def save_candles(self, candles_df, pair, granularity):
        candles_df.to_pickle(f"his_data/{pair}_{granularity}.pkl")
        return

    def create_candle_data(self, pair, granularity):
        code, json_data = self.fetch_candles(pair, 5000, granularity)
        # print(code, json_data)
        if code != 200:
            print(pair, f"HTTP Error {code}")
            return
        candle_df = self.fetch_candles_df(json_data)
        print(f"{pair} loaded {candle_df.shape[0]} candles from {candle_df.time.min()} to {candle_df.time.max()}")
        self.save_candles(candle_df, pair, granularity)
        return

    @classmethod
    def candles_to_df(cls, json_data):
        prices = ['mid', 'bid', 'ask']
        ohlc = ['o', 'h', 'l', 'c']
        our_data = []
        for candle in json_data:
            if candle['complete'] == False:
                continue
            new_dict = {}
            new_dict['time'] = candle['time']
            new_dict['volume'] = candle['volume']
            for price in prices:
                for oh in ohlc:
                    new_dict[f"{price}_{oh}"] = float(candle[price][oh])
                our_data.append(new_dict)
        df = pd.DataFrame.from_dict(our_data)    
        df["time"] = [parse(x) for x in df.time]
        return df

if __name__ == "__main__":
    our_curr = ['EUR', 'USD', 'GBP', 'JPY', 'CHF', 'NZD', 'CAD']
    api = OandaAPI()
    # api.save_instruments()
    
    date_from = oanda_utils.get_utc_dt_from_string("2019-05-05 18:00:00")
    date_to = oanda_utils.get_utc_dt_from_string("2019-05-25 18:00:00")
    res, df = api.fetch_candles("EUR_USD", date_from=date_from, date_to=date_to, as_df=True)
    print(df.info())
