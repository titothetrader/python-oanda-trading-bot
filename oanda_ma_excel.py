import pandas as pd
import xlsxwriter

def get_line_chart(book, start_row, end_row, labels_col, data_col, title, sheetname):
    chart = book.add_chart({'type': 'line'})
    chart.add_series({
        'categories' : [sheetname, start_row, labels_col, end_row, labels_col],
        'values' : [sheetname, start_row, data_col, end_row, data_col],
        'line': {'color' : 'blue'}
    })

    chart.set_legend({'none': True})
    return chart

def add_chart(pair_name, cross, df, writer):
    workbook = writer.book
    worksheet = writer.sheets[pair_name]

    chart = get_line_chart(workbook, 1, df.shape[0], 8, 9, f"Cum. Gains for {pair_name} {cross}", pair_name)
    chart.set_size({'x_scale': 2.5, 'y_scale': 2.5})
    worksheet.insert_chart('K1', chart)

def add_pair_charts(ma_test_res, all_trades, writer):
    cols = ["time", "CUM_GAIN"]
    # print(ma_test_res.head(10))
    df_temp = ma_test_res.drop_duplicates(subset=["pair"])
    # print(df_temp.head(5))

    for index, row in df_temp.iterrows():
        # print("index:", index)
        # print("row:", row.CROSS, row.pair)
        temp_all_trades = all_trades[(all_trades.CROSS==row.CROSS) & (all_trades.PAIR==row.pair)].copy()
        
        temp_all_trades['CUM_GAIN'] = temp_all_trades.GAIN.cumsum()
        # print(temp_all_trades.tail(5))
        temp_all_trades[cols].to_excel(writer, sheet_name=row.pair, startrow=0, startcol=7)
        add_chart(row.pair, row.CROSS, temp_all_trades, writer)

def add_pair_sheets(ma_test_res, writer):
    for p in ma_test_res.pair.unique():
        temp_df = ma_test_res[ma_test_res.pair==p]
        temp_df.to_excel(writer, sheet_name=p, index=False)

def create_excel(ma_test_res, all_trades):
    filename = "ma_results.xlsx"
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')

    ma_test_res = ma_test_res[['pair', 'num_trades', 'total_gain', 'mashort', 'malong']].copy()
    ma_test_res["CROSS"] = "MA_" + ma_test_res.mashort.map(str) + "_" + ma_test_res.malong.map(str)
    ma_test_res.sort_values(by=["pair", "total_gain"], ascending=[True,False], inplace=True)

    all_trades["CROSS"] = "MA_" + all_trades.MASHORT.map(str) + "_" + all_trades.MALONG.map(str)
    all_trades["time"] = [x.replace(tzinfo=None) for x in all_trades.time]      # Remove all timezone info, for Excel ready

    add_pair_sheets(ma_test_res, writer)
    add_pair_charts(ma_test_res, all_trades, writer)
    writer.save()

if __name__ == "__main__":
    ma_test_res = pd.read_pickle("ma_test_res.pkl")
    all_trades = pd.read_pickle("all_trades.pkl")
    create_excel(ma_test_res, all_trades)