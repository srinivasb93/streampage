from common_utils import read_write_sql_data as rd

agg_data = rd.get_table_data(selected_table='AGG_DATA')
eod_data = rd.get_table_data(selected_table='EOD_Summary')

eod_data_cols = ['Date', 'Symbol', 'Pct_Chg_5D', 'Pct_Chg_20D', 'Pct_Chg_365D', 'HH', 'LL',
                   'High_20', 'Low_20', 'ATR', 'Range_ATR', 'Vol_Avg20', 'EMA_20',
                   'EMA_60', 'EMA_200', 'Reg_6', 'Reg_18', 'Reg_6_Chg', 'Reg_Cross',
                   'Vol_Abv_Avg20', 'Cls_Abv_EMA20', 'Cls_Abv_EMA60', 'Cls_Abv_EMA200',
                   'Cls_Abv_Reg6', 'Curr_Supp', 'Prev_Supp', 'Curr_Res', 'Prev_Res',
                   'Resistance', 'Support', 'EMA20_Sig', 'EMA20_Cnt', 'Reg6_Sig',
                   'Vol20_Sig', 'High_Low', 'Break_Sup_Res',
                   'Reg_Cross_Sig', 'Breakout_20', 'ATR_20', 'Range_Pct',
                   'Avg_Range_Pct_20', 'Narrow_Range', 'Narrow_Range_Count',
                   'Narrow_Range_Breakout']

eod_df = eod_data[eod_data_cols].copy()

# Merge the dataframes on Symbol and Date
merged_df = agg_data.join(eod_df, rsuffix='_right', how='outer').reset_index()

print(agg_data.columns)
print(eod_data.columns)