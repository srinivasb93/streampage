import common_utils
from datetime import datetime
import time
import threading


def auto_trade(api, strategy_name, instrument_token, quantity, stop_loss_percent=1.0, take_profit_percent=2.0):
    try:
        data = common_utils.get_historical_data(api, instrument_token)
        if data is None:
            return "Failed to fetch historical data"

        signal = None
        if strategy_name == "MACD Crossover":
            macd_line, signal_line, _ = common_utils.calculate_macd(data)
            signal = common_utils.check_macd_crossover(macd_line, signal_line)
        elif strategy_name == "Bollinger Bands":
            _, upper_band, lower_band = common_utils.calculate_bollinger_bands(data)
            signal = common_utils.check_bollinger_band_signals(data, upper_band, lower_band)
        elif strategy_name == "RSI Oversold/Overbought":
            rsi = common_utils.calculate_rsi(data)
            if rsi.iloc[-1] < 30:
                signal = "BUY"
            elif rsi.iloc[-1] > 70:
                signal = "SELL"
        elif strategy_name == "Stochastic Oscillator":
            k, d = common_utils.calculate_stochastic_oscillator(data)
            signal = common_utils.check_stochastic_signals(k, d)
        elif strategy_name == "Support/Resistance Breakout":
            signal = common_utils.check_support_resistance_breakout(data)

        if signal:
            result = common_utils.place_order(api, instrument_token, signal, quantity)
            if result and result.data and result.data.order_id:
                current_price = data['close'].iloc[-1]
                if signal == "BUY":
                    stop_loss_price = current_price * (1 - stop_loss_percent / 100)
                    take_profit_price = current_price * (1 + take_profit_percent / 100)
                    common_utils.place_order(api, instrument_token, "SELL", quantity, price=0,
                                             order_type="SL-M", trigger_price=stop_loss_price)
                    common_utils.place_order(api, instrument_token, "SELL", quantity,
                                             price=take_profit_price, order_type="LIMIT")
                elif signal == "SELL":
                    stop_loss_price = current_price * (1 + stop_loss_percent / 100)
                    take_profit_price = current_price * (1 - take_profit_percent / 100)
                    common_utils.place_order(api, instrument_token, "BUY", quantity, price=0,
                                             order_type="SL-M", trigger_price=stop_loss_price)
                    common_utils.place_order(api, instrument_token, "BUY", quantity,
                                             price=take_profit_price, order_type="LIMIT")
                return f"{signal} signal detected and executed with stop loss and take profit"
            return f"{signal} signal detected but order failed"
        return "No trading signal detected"
    except Exception as e:
        return f"Error in auto_trade: {str(e)}"


def schedule_strategy_execution(api, strategy_name, instrument_token, quantity, interval_minutes=5, run_hours=None):
    if run_hours is None:
        run_hours = [(9, 15), (15, 30)]

    def is_market_open():
        now = datetime.now()
        weekday = now.weekday()
        if weekday >= 5:
            return False
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
            time.sleep(interval_minutes * 60)

    strategy_thread = threading.Thread(target=run_strategy, daemon=True)
    strategy_thread.start()
    return "Strategy scheduled successfully"
