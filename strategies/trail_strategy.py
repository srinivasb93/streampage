from backtesting import Backtest
from backtesting import Strategy
from backtesting.test import GOOG
from backtesting.lib import crossover, TrailingStrategy

import ta

def RsiIndicator(close, window):
    rsi = ta.momentum.RSIIndicator(close, window=window)
    return rsi.rsi()

class RsiOscillator(TrailingStrategy):
    """
    Buy when we cross the lower bound to the upside
    hold until we get stopped out
    """

    rsi_window = 14
    lower_bound = 70
    stop_range = 10

    def init(self):
        super().init()
        super().set_trailing_sl(self.stop_range)
        self.rsi = self.I(RsiIndicator, self.data.Close.s, self.rsi_window)

    def next(self):
        super().next()

        if len(self.trades) == 0:
            if crossover(self.rsi, self.lower_bound):
                self.buy()

bt = Backtest(GOOG, RsiOscillator, cash = 10_000, commission=0.002)

stats = bt.run(
        rsi_window = 14,
        lower_bound = 50,
        stop_range = 5,
        )

print(stats)
bt.plot()
