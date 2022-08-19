class MAResult():
    def __init__(self, df_trades, pair_name, params):
        self.pair_name = pair_name
        self.df_trades = df_trades
        self.params = params

    def result_ob(self):
        d = {
            'pair' : self.pair_name,
            'num_trades' : self.df_trades.shape[0],
            'total_gain' : self.df_trades.GAIN.sum(),
            'mean_gain' : self.df_trades.GAIN.mean(),
            'min_gain' : self.df_trades.GAIN.min(),
            'max_gain' : self.df_trades.GAIN.max(),
        }

        for k,v in self.params.items():
            d[k] = v
        return d