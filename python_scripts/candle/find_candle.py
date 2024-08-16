def find_candle(df, duration='D'):
    for rdata in df.itertuples():
        k = rdata.Index + 1
        if k < len(df.index):
            upp_shadow = (df.loc[k, 'High'] - df.loc[k, 'Close']) if df.loc[k, 'Close'] > df.loc[k, 'Open'] \
                else (df.loc[k, 'High'] - df.loc[k, 'Open'])
            low_shadow = (df.loc[k, 'Open'] - df.loc[k, 'Low']) if df.loc[k, 'Close'] > df.loc[k, 'Open'] \
                else (df.loc[k, 'Close'] - df.loc[k, 'Low'])
            # mid_body = (df.loc[k,'Close'] - df.loc[k,'Open']) if df.loc[k,'Close'] > df.loc[k,'Open'] else (df.loc[k,'Open'] - df.loc[k,'Close'])
            body = df.loc[k, 'Close'] - df.loc[k, 'Open']
            upp_by_low = upp_shadow/(low_shadow if low_shadow != 0 else .0001)
            low_by_upp = low_shadow/(upp_shadow if upp_shadow != 0 else .0001)
            low_by_body = low_shadow/(body if body != 0 else .0001)
            upp_by_body = upp_shadow/(body if body != 0 else .0001)

            # df.loc[k, 'Upp_Shd'] = round(upp_shadow, 2)
            # df.loc[k, 'Low_Shd'] = round(low_shadow, 2)
            # df.loc[k, 'Body'] = round(body, 2)
            body_by_range = round(body/df.loc[k, 'Range_'+duration], 2)

            if low_shadow != 0 and upp_shadow != 0 and low_by_upp >= 3 and abs(low_by_body) > 1:
                df.loc[k, 'Candle_'+duration] = 'Bullish_Hammer_G' if body > 0 else 'Bullish_Hammer_R'
            if low_shadow != 0 and upp_shadow != 0 and upp_by_low >= 3 and abs(upp_by_body) > 1:
                df.loc[k, 'Candle_'+duration] = 'Inverted_Hammer_G' if body > 0 else 'Inverted_Hammer_R'

            if body != 0 and abs(upp_by_body) < .5 and abs(low_by_body) < .5:
                df.loc[k, 'Candle_'+duration] = 'Bullish' if body > 0 else 'Bearish'

            if low_shadow != 0 and upp_shadow != 0 and -.1 <= body_by_range <= .1 and .65 <= upp_by_low <= 1.25:
                df.loc[k, 'Candle_'+duration] = 'Doji'

