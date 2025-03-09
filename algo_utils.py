import pandas as pd

# Define algorithmic trading strategies
def calculate_macd(df, fast_period=12, slow_period=26, signal_period=9):
    """Calculate MACD indicator."""
    # Calculate EMAs
    fast_ema = df['close'].ewm(span=fast_period, adjust=False).mean()
    slow_ema = df['close'].ewm(span=slow_period, adjust=False).mean()

    # Calculate MACD line and signal line
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()

    # Calculate histogram
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


def calculate_bollinger_bands(df, period=20, num_std=2):
    """Calculate Bollinger Bands."""
    # Calculate rolling mean and standard deviation
    rolling_mean = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()

    # Calculate upper and lower bands
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)

    return rolling_mean, upper_band, lower_band


def calculate_stochastic_oscillator(df, k_period=14, d_period=3):
    """Calculate Stochastic Oscillator."""
    # Calculate %K
    low_min = df['low'].rolling(window=k_period).min()
    high_max = df['high'].rolling(window=k_period).max()
    k = 100 * ((df['close'] - low_min) / (high_max - low_min))

    # Calculate %D (signal line)
    d = k.rolling(window=d_period).mean()

    return k, d


def check_macd_crossover(macd_line, signal_line):
    """Check for MACD crossover signals."""
    if len(macd_line) < 2 or len(signal_line) < 2:
        return None

    # Check for bullish crossover (MACD crosses above signal line)
    if macd_line.iloc[-2] < signal_line.iloc[-2] and macd_line.iloc[-1] > signal_line.iloc[-1]:
        return "BUY"

    # Check for bearish crossover (MACD crosses below signal line)
    elif macd_line.iloc[-2] > signal_line.iloc[-2] and macd_line.iloc[-1] < signal_line.iloc[-1]:
        return "SELL"

    return None


def check_bollinger_band_signals(df, upper_band, lower_band):
    """Check for Bollinger Band signals."""
    if len(df) < 2:
        return None

    # Price breaks above upper band (overbought)
    if df['close'].iloc[-2] <= upper_band.iloc[-2] and df['close'].iloc[-1] > upper_band.iloc[-1]:
        return "SELL"

    # Price breaks below lower band (oversold)
    elif df['close'].iloc[-2] >= lower_band.iloc[-2] and df['close'].iloc[-1] < lower_band.iloc[-1]:
        return "BUY"

    return None


def check_stochastic_signals(k, d, overbought=80, oversold=20):
    """Check for Stochastic Oscillator signals."""
    if len(k) < 2 or len(d) < 2:
        return None

    # Oversold condition with bullish crossover
    if (k.iloc[-2] < d.iloc[-2] and k.iloc[-1] > d.iloc[-1]) and k.iloc[-1] < oversold:
        return "BUY"

    # Overbought condition with bearish crossover
    elif (k.iloc[-2] > d.iloc[-2] and k.iloc[-1] < d.iloc[-1]) and k.iloc[-1] > overbought:
        return "SELL"

    return None


def check_support_resistance_breakout(df, lookback=20):
    """Identify support and resistance breakouts."""
    if len(df) < lookback + 2:
        return None

    recent_high = df['high'].iloc[-lookback:-2].max()
    recent_low = df['low'].iloc[-lookback:-2].min()

    # Breakout above resistance
    if df['close'].iloc[-2] < recent_high and df['close'].iloc[-1] > recent_high:
        return "BUY"

    # Breakdown below support
    elif df['close'].iloc[-2] > recent_low and df['close'].iloc[-1] < recent_low:
        return "SELL"

    return None


# Backtest a strategy
def backtest_strategy(df, strategy_func, **kwargs):
    """Simple backtest framework."""
    # Make a copy of the dataframe
    df_copy = df.copy()

    # Apply strategy and get signals
    signals = strategy_func(df_copy, **kwargs)

    # Add signals to dataframe
    df_copy['signal'] = signals

    # Initialize positions and pnl columns
    df_copy['position'] = 0
    df_copy['pnl'] = 0

    # Calculate positions based on signals
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

    # Calculate cumulative PnL
    df_copy['cumulative_pnl'] = df_copy['pnl'].cumsum()

    return df_copy


# Strategy implementations
def macd_strategy(df, fast_period=12, slow_period=26, signal_period=9):
    """MACD crossover strategy."""
    macd_line, signal_line, _ = calculate_macd(df, fast_period, slow_period, signal_period)

    # Initialize signals
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None

    # Find crossovers
    for i in range(1, len(df)):
        if i > signal_period:
            if macd_line.iloc[i - 1] < signal_line.iloc[i - 1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.iloc[i] = "BUY"
            elif macd_line.iloc[i - 1] > signal_line.iloc[i - 1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.iloc[i] = "SELL"

    return signals


def bollinger_band_strategy(df, period=20, num_std=2):
    """Bollinger Bands mean reversion strategy."""
    _, upper_band, lower_band = calculate_bollinger_bands(df, period, num_std)

    # Initialize signals
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None

    # Find band touches
    for i in range(1, len(df)):
        if i > period:
            if df['close'].iloc[i] < lower_band.iloc[i]:
                signals.iloc[i] = "BUY"
            elif df['close'].iloc[i] > upper_band.iloc[i]:
                signals.iloc[i] = "SELL"

    return signals


def rsi_strategy(df, period=14, overbought=70, oversold=30):
    """RSI strategy."""
    rsi = calculate_rsi(df, period)

    # Initialize signals
    signals = pd.Series(index=df.index, dtype='object')
    signals[:] = None

    # Find overbought/oversold conditions
    for i in range(1, len(df)):
        if i > period:
            if rsi.iloc[i - 1] > overbought and rsi.iloc[i] <= overbought:
                signals.iloc[i] = "SELL"
            elif rsi.iloc[i - 1] < oversold and rsi.iloc[i] >= oversold:
                signals.iloc[i] = "BUY"

    return signals


# Automated trading function
def auto_trade(api, strategy_name, instrument_token, quantity, stop_loss_percent=1.0, take_profit_percent=2.0):
    """Execute automated trading based on selected strategy."""
    try:
        # Get historical data
        data = get_historical_data(api, instrument_token)
        if data is None:
            return "Failed to fetch historical data"

        # Apply strategy
        signal = None

        if strategy_name == "MACD Crossover":
            macd_line, signal_line, _ = calculate_macd(data)
            signal = check_macd_crossover(macd_line, signal_line)

        elif strategy_name == "Bollinger Bands":
            _, upper_band, lower_band = calculate_bollinger_bands(data)
            signal = check_bollinger_band_signals(data, upper_band, lower_band)

        elif strategy_name == "RSI Oversold/Overbought":
            rsi = calculate_rsi(data)
            if rsi.iloc[-1] < 30:
                signal = "BUY"
            elif rsi.iloc[-1] > 70:
                signal = "SELL"

        elif strategy_name == "Stochastic Oscillator":
            k, d = calculate_stochastic_oscillator(data)
            signal = check_stochastic_signals(k, d)

        elif strategy_name == "Support/Resistance Breakout":
            signal = check_support_resistance_breakout(data)

        # Execute trade if there's a signal
        if signal:
            result = place_order(api, instrument_token, signal, quantity)

            # Set stop loss and take profit orders
            if result and result.data and result.data.order_id:
                current_price = data['close'].iloc[-1]

                if signal == "BUY":
                    stop_loss_price = current_price * (1 - stop_loss_percent / 100)
                    take_profit_price = current_price * (1 + take_profit_percent / 100)

                    # Place stop loss order
                    place_order(api, instrument_token, "SELL", quantity,
                                price=0, order_type="SL-M", trigger_price=stop_loss_price)

                    # Place take profit order
                    place_order(api, instrument_token, "SELL", quantity,
                                price=take_profit_price, order_type="LIMIT")

                elif signal == "SELL":
                    stop_loss_price = current_price * (1 + stop_loss_percent / 100)
                    take_profit_price = current_price * (1 - take_profit_percent / 100)

                    # Place stop loss order
                    place_order(api, instrument_token, "BUY", quantity,
                                price=0, order_type="SL-M", trigger_price=stop_loss_price)

                    # Place take profit order
                    place_order(api, instrument_token, "BUY", quantity,
                                price=take_profit_price, order_type="LIMIT")

                return f"{signal} signal detected and executed with stop loss and take profit"

            return f"{signal} signal detected but order failed"

        return "No trading signal detected"

    except Exception as e:
        return f"Error in auto_trade: {str(e)}"


# Scheduled trading function
def schedule_strategy_execution(api, strategy_name, instrument_token, quantity,
                                interval_minutes=5, run_hours=None):
    """Schedule a strategy to run at regular intervals."""
    if run_hours is None:
        run_hours = [(9, 15), (15, 30)]  # Default market hours

    def is_market_open():
        now = datetime.now()
        weekday = now.weekday()

        # Check if weekend (5=Saturday, 6=Sunday)
        if weekday >= 5:
            return False

        # Check if within trading hours
        for start_hour, end_hour in run_hours:
            start_time = now.replace(hour=start_hour, minute=0, second=0)
            end_time = now.replace(hour=end_hour, minute=0, second=0)
            if start_time <= now <= end_time:
                return True

        return False

    def run_strategy():
        while True:
            if is_market_open():
                result = auto_trade(api, strategy_name, instrument_token, quantity)
                print(f"Strategy execution result: {result}")
            else:
                print("Market closed. Waiting for next interval.")

            # Wait for the next interval
            time.sleep(interval_minutes * 60)

    # Start the strategy execution in a background thread
    strategy_thread = threading.Thread(target=run_strategy, daemon=True)
    strategy_thread.start()

    return "Strategy scheduled successfully"