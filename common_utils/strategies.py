import pandas as pd
import common_utils


def check_macd_crossover(macd_line, signal_line):
    if len(macd_line) < 2 or len(signal_line) < 2:
        return None
    if macd_line.iloc[-2] < signal_line.iloc[-2] and macd_line.iloc[-1] > signal_line.iloc[-1]:
        return "BUY"
    elif macd_line.iloc[-2] > signal_line.iloc[-2] and macd_line.iloc[-1] < signal_line.iloc[-1]:
        return "SELL"
    return None


def check_bollinger_band_signals(df, upper_band, lower_band):
    if len(df) < 2:
        return None
    if df['close'].iloc[-2] <= upper_band.iloc[-2] and df['close'].iloc[-1] > upper_band.iloc[-1]:
        return "SELL"
    elif df['close'].iloc[-2] >= lower_band.iloc[-2] and df['close'].iloc[-1] < lower_band.iloc[-1]:
        return "BUY"
    return None


def check_stochastic_signals(k, d, overbought=80, oversold=20):
    if len(k) < 2 or len(d) < 2:
        return None
    if (k.iloc[-2] < d.iloc[-2] and k.iloc[-1] > d.iloc[-1]) and k.iloc[-1] < oversold:
        return "BUY"
    elif (k.iloc[-2] > d.iloc[-2] and k.iloc[-1] < d.iloc[-1]) and k.iloc[-1] > overbought:
        return "SELL"
    return None


def check_support_resistance_breakout(df, lookback=20):
    if len(df) < lookback + 2:
        return None
    recent_high = df['high'].iloc[-lookback:-2].max()
    recent_low = df['low'].iloc[-lookback:-2].min()
    if df['close'].iloc[-2] < recent_high and df['close'].iloc[-1] > recent_high:
        return "BUY"
    elif df['close'].iloc[-2] > recent_low and df['close'].iloc[-1] < recent_low:
        return "SELL"
    return None


def backtest_strategy(df, strategy_func, **kwargs):
    df_copy = df.copy()
    signals = strategy_func(df_copy, **kwargs)
    df_copy['signal'] = signals
    df_copy['position'] = 0
    df_copy['pnl'] = 0
    position = 0
    buy_price = 0

    for i, row in df_copy.iterrows():
        if row['signal'] == "BUY" and position == 0:
            position = 1
            buy_price = row['close']
            df_copy.at[i, 'position'] = position
        elif row['signal'] == "SELL" and position == 1:
            position = 0
            sell_price = row['close']
            df_copy.at[i, 'position'] = position
            df_copy.at[i, 'pnl'] = sell_price - buy_price

    df_copy['cumulative_pnl'] = df_copy['pnl'].cumsum()
    return df_copy


def macd_strategy(df, fast_period=12, slow_period=26, signal_period=9):
    macd_line, signal_line, _ = common_utils.calculate_macd(df, fast_period, slow_period, signal_period)
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None
    for i in range(1, len(df)):
        if i > signal_period:
            if macd_line.iloc[i - 1] < signal_line.iloc[i - 1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.iloc[i] = "BUY"
            elif macd_line.iloc[i - 1] > signal_line.iloc[i - 1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.iloc[i] = "SELL"
    return signals


def bollinger_band_strategy(df, period=20, num_std=2):
    _, upper_band, lower_band = common_utils.calculate_bollinger_bands(df, period, num_std)
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None
    for i in range(1, len(df)):
        if i > period:
            if df['close'].iloc[i] < lower_band.iloc[i]:
                signals.iloc[i] = "BUY"
            elif df['close'].iloc[i] > upper_band.iloc[i]:
                signals.iloc[i] = "SELL"
    return signals


def rsi_strategy(df, period=14, overbought=70, oversold=30):
    rsi = common_utils.calculate_rsi(df, period)
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None
    for i in range(1, len(df)):
        if i > period:
            if rsi.iloc[i - 1] > overbought and rsi.iloc[i] <= overbought:
                signals.iloc[i] = "SELL"
            elif rsi.iloc[i - 1] < oversold and rsi.iloc[i] >= oversold:
                signals.iloc[i] = "BUY"
    return signals
