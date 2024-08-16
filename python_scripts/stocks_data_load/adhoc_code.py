import numpy as np

class RangeBreakoutStrategy:
    def __init__(self, price_data):
        self.price_data = price_data
        self.high = np.max(price_data)
        self.low = np.min(price_data)

    def is_breakout(self):
        if self.price_data[-1] > self.high:
            return True
        elif self.price_data[-1] < self.low:
            return True
        else:
            return False

    def get_entry_price(self):
        if self.is_breakout():
            return self.price_data[-1]
        else:
            return None

    def get_stop_loss_price(self):
        if self.is_breakout():
            return self.high if self.price_data[-1] > self.high else self.low
        else:
            return None

def main():
    price_data = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
    strategy = RangeBreakoutStrategy(price_data)

    if strategy.is_breakout():
        print("Range breakout detected!")
        print("Entry price:", strategy.get_entry_price())
        print("Stop loss price:", strategy.get_stop_loss_price())
    else:
        print("No range breakout detected.")

if __name__ == "__main__":
    main()