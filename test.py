import pandas as pd
import numpy as np

RETRACEMENT_MATRIX_1 = [
    {"lower": 0.884, "upper": 0.854, "entry": 0.702, "sl": 0.884, "target": 0.44},
    {"lower": 0.854, "upper": 0.786, "entry": 0.618, "sl": 0.854, "target": 0.382},
    {"lower": 0.786, "upper": 0.618, "entry": 0.5, "sl": 0.786, "target": 0.236},
    {"lower": 0.618, "upper": 0.5, "entry": 0.44, "sl": 0.702, "target": -0.272},
    {"lower": 0.5, "upper": 0.382, "entry": 0.309, "sl": 0.575, "target": -0.382},
    {"lower": 0.44, "upper": 0.236, "entry": 0.163, "sl": 0.44, "target": -0.618},
    {"lower": 0.236, "upper": 0.146, "entry": 0.073, "sl": 0.309, "target": -1},
]

RETRACEMENT_MATRIX = [
    {"lower": 0.884, "upper": 0.854, "entry": 0.702, "sl": 0.884, "target": 0.44},
    {"lower": 0.854, "upper": 0.786, "entry": 0.618, "sl": 0.854, "target": 0.382},
    {"lower": 0.786, "upper": 0.618, "entry": 0.5, "sl": 0.786, "target": 0.236},
    {"lower": 0.618, "upper": 0.5, "entry": 0.44, "sl": 0.702, "target": 0.146},
    {"lower": 0.5, "upper": 0.382, "entry": 0.309, "sl": 0.575, "target": 0},
    {"lower": 0.44, "upper": 0.236, "entry": 0.163, "sl": 0.44, "target": -0.272},
    {"lower": 0.236, "upper": 0.146, "entry": 0.073, "sl": 0.309, "target": -0.382},
]


class OptionsStrikePriceTrader():
    def __init__(self, token, historical_df, dhan_user_obj):
        self.token = token
        self.historical_df = historical_df.copy()

        # Proper timestamp conversion to UTC
        self.historical_df['timestamp'] = pd.to_datetime(self.historical_df['timestamp'])

        if self.historical_df['timestamp'].dt.tz is None:
            # If timestamps are naive, assume Asia/Kolkata and convert to UTC
            self.historical_df['timestamp'] = self.historical_df['timestamp'].dt.tz_localize('Asia/Kolkata').dt.tz_convert('UTC')
        else:
            # If already timezone-aware, convert to UTC directly
            self.historical_df['timestamp'] = self.historical_df['timestamp'].dt.tz_convert('UTC')

        self.dhan_user_obj = dhan_user_obj

        # State variables
        self.swing_high = None
        self.swing_low = None
        self.retracement_found = False
        self.last_processed_idx = None
        self.target = None
        self.stop_loss = None
        self.entry_zone = None

        # Initialize swings from historical data
        self.detect_swings()

        if self.swing_high and self.swing_low:
            print(f"[Init] Swing High: {self.swing_high}, Swing Low: {self.swing_low}")
        else:
            print("[Init] No valid swing found.")


    def detect_swings(self, total_candles=64, groups=8):
        if len(self.historical_df) < total_candles + 1:
            return None, None

        df_window = self.historical_df.iloc[-(total_candles + 1):-1]

        groups_split = np.array_split(df_window[::-1], groups)

        swing_high = None
        swing_high_group_idx = None

        for idx, group in enumerate(groups_split):
            group_high = group['high'].max()
            if swing_high is None or group_high > swing_high:
                swing_high = group_high
                swing_high_group_idx = idx

        swing_low = None
        for group in groups_split[swing_high_group_idx + 1:]:
            group_low = group['low'].min()
            if swing_low is None or group_low < swing_low:
                swing_low = group_low

        if swing_high and swing_low:
            self.swing_high = swing_high
            self.swing_low = swing_low
            self.retracement_found = False
            print(f"[Init] Swing High: {self.swing_high}, Swing Low: {self.swing_low}")
        else:
            print("[Init] Not enough data to detect swings.")

    def add_live_data(self, open_, high, low, close, timestamp):
        ts = pd.to_datetime(timestamp)

        if ts.tzinfo is None:
            # If incoming timestamp is naive, assume Asia/Kolkata and convert to UTC
            ts = ts.tz_localize('Asia/Kolkata').tz_convert('UTC')
        else:
            # If timestamp is tz-aware, convert directly to UTC
            ts = ts.tz_convert('UTC')

        new_row = pd.DataFrame([{
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': None,
            'timestamp': ts
        }])

        self.historical_df = pd.concat([self.historical_df, new_row], ignore_index=True)

        # Ensure timestamp remains UTC (no need to call to_datetime again)
        self.historical_df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        self.historical_df.sort_values('timestamp', inplace=True)
        self.historical_df.reset_index(drop=True, inplace=True)

    def calculate_multi_rsi(self):
        """
        Calculates 3, 5, and 10-minute RSI using pandas-ta.
        Assumes `self.historical_df` has UTC timestamps.
        """
        df = self.historical_df.copy()

        # Ensure timestamp is datetime and set as index
        df = df.set_index('timestamp').sort_index()

        rsi_results = {}

        for timeframe in [3, 5, 10]:
            resampled = df.resample(f'{timeframe}min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            rsi_col = f'RSI_{timeframe}min'
            resampled[rsi_col] = ta.rsi(resampled['close'], length=14)

            # Save the latest RSI value
            latest_rsi = resampled[rsi_col].dropna().iloc[-1] if not resampled[rsi_col].dropna().empty else None
            rsi_results[timeframe] = latest_rsi

        return rsi_results



    def check_retracement_or_reset(self):
        """
        Checks for retracement after swing high.
        - If new swing high is made, resets swings.
        - If retracement is found, returns target, stoploss, entry.
        """
        latest = self.historical_df.iloc[-1]

        # Check if new high is broken -> reset swing detection
        if latest['high'] > self.swing_high:
            print(f"[New High] {latest['high']} > {self.swing_high}. Resetting swings.")
            self.detect_swings()
            return None

        # If retracement already processed, skip
        if self.retracement_found:
            return None

        diff = self.swing_high - self.swing_low
        retracement = (self.swing_high - latest['close']) / diff

        for zone in RETRACEMENT_MATRIX:
            if zone['upper'] < retracement < zone['lower']:
                self.retracement_found = True

                entry = self.swing_high - zone['entry'] * diff
                sl = self.swing_high - zone['sl'] * diff
                target = self.swing_high - zone['target'] * diff

                print(f"[Retracement Found] Retracement: {retracement:.3f}")
                print(f"Entry: {entry:.2f}, SL: {sl:.2f}, Target: {target:.2f}")

                return {
                    "retracement": retracement,
                    "entry": entry,
                    "stop_loss": sl,
                    "target": target,
                    "swing": (self.swing_low, self.swing_high),
                    "zone": zone
                }

        # No retracement found yet
        return None

if __name__ == "__main__":
    hist_data = pd.read_csv('54086.csv').head(2625)
    live = pd.read_csv('54086.csv').tail(375)
    obj=OptionsStrikePriceTrader(token='54086', historical_df=hist_data, dhan_user_obj=None)

    for idx, data in live.iterrows():
        obj.add_live_data(
            open_=data['open'],
            high=data['high'],
            low=data['low'],
            close=data['close'],
            timestamp=data['timestamp']
        ) 
        res=obj.check_retracement_or_reset()
        open("result.txt", "a").write(data['timestamp']+str(res) + "\n")

    
