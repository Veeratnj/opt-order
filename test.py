import pandas as pd
import numpy as np
import pandas_ta as ta

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
        self.swing_high = 0
        self.swing_low = 0
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


    def detect_swings_1(self, total_candles=64, groups=8):
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

    def detect_swings_2(self, lookback=60):
        if len(self.historical_df) < lookback:
            print("[Init] Not enough data to detect swings.")
            return None, None

        df_window = self.historical_df.iloc[-lookback:]

        max_high = df_window['high'].max()
        max_high_idx = df_window['high'].idxmax()
        # swing_high_time = df_window.loc[max_high_idx].name  # Use .name if index is datetime
        # If you have a 'timestamp' column, use:
        # swing_high_time = df_window.loc[max_high_idx]['timestamp']

        # Take left side of max high
        max_high_pos = df_window.index.get_loc(max_high_idx)
        left_df = df_window.iloc[:max_high_pos]

        if left_df.empty:
            print("[Init] No left side data to find swing low.")
            return None, None

        min_low = left_df['low'].min()
        min_low_idx = left_df['low'].idxmin()
        swing_high_time = df_window.loc[max_high_idx]['timestamp']
        swing_low_time = df_window.loc[min_low_idx]['timestamp']
        # Or use: swing_low_time = left_df.loc[min_low_idx]['timestamp']

        # Store results
        self.swing_high = max_high
        self.swing_low = min_low
        self.retracement_found = False

        print(f"[Init] Swing High: {self.swing_high} at {swing_high_time} ")
        print(f"[Init] Swing Low: {self.swing_low} at {swing_low_time} ")

        return self.swing_high, self.swing_low

    def detect_swings_curr(self, lookback=60):
        if len(self.historical_df) < lookback:
            print("[Init] Not enough data to detect swings.")
            return None, None

        df_window = self.historical_df.iloc[-lookback:]

        # Step 1: Find swing low (lowest low in lookback window)
        min_low_idx = df_window['low'].idxmin()
        swing_low = df_window.at[min_low_idx, 'low']
        swing_low_time = df_window.at[min_low_idx, 'timestamp']

        # Step 2: Take right side of min_low to find next swing high
        min_low_pos = df_window.index.get_loc(min_low_idx)
        right_df = df_window.iloc[min_low_pos + 1:]

        if right_df.empty:
            print("[Init] No right side data to find swing high.")
            return None, None

        # Step 3: Find swing high (after swing low)
        max_high_idx = right_df['high'].idxmax()
        swing_high = right_df.at[max_high_idx, 'high']
        swing_high_time = right_df.at[max_high_idx, 'timestamp']

        # Store results
        self.swing_low = swing_low
        self.swing_high = swing_high
        self.retracement_found = False

        print(f"[Init] Swing Low: {self.swing_low} at {swing_low_time}")
        print(f"[Init] Swing High: {self.swing_high} at {swing_high_time}")

        return self.swing_low, self.swing_high


    def detect_swings(self, lookback=60):
        if len(self.historical_df) < lookback:
            print("[Init] Not enough data to detect swings.")
            return None, None

        df_window = self.historical_df.iloc[-lookback:]
        print(df_window.head())

        # Step 1: Find swing low (lowest low in lookback window)
        min_low_idx = df_window['low'].idxmin()  # DataFrame index (not position)
        swing_low = df_window.at[min_low_idx, 'low']
        swing_low_time = df_window.at[min_low_idx, 'timestamp']

        # Save swing low index
        self.swing_low_index = min_low_idx

        # Step 2: Take right side of min_low to find next swing high
        min_low_pos = df_window.index.get_loc(min_low_idx)
        right_df = df_window.iloc[min_low_pos + 1:]

        if right_df.empty:
            print("[Init] No right side data to find swing high.")
            return None, None

        # Step 3: Find swing high (after swing low)
        max_high_idx = right_df['high'].idxmax()
        swing_high = right_df.at[max_high_idx, 'high']
        swing_high_time = right_df.at[max_high_idx, 'timestamp']

        # Save swing high index
        self.swing_high_index = max_high_idx

        # Store swing values
        self.swing_low = swing_low
        self.swing_high = swing_high
        self.retracement_found = False

        print(f"[Init] Swing Low: {self.swing_low} at {swing_low_time} (index: {self.swing_low_index})")
        print(f"[Init] Swing High: {self.swing_high} at {swing_high_time} (index: {self.swing_high_index})")

        return self.swing_low, self.swing_high



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

    def calculate_multi_rsi12(self):
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

            rsi_col = f'RSI_{timeframe}m'
            resampled[rsi_col] = ta.rsi(resampled['close'], length=14)

            # Save the latest RSI value
            latest_rsi = resampled[rsi_col].dropna().iloc[-1] if not resampled[rsi_col].dropna().empty else None
            rsi_results[timeframe] = latest_rsi

        return rsi_results

    def calculate_multi_rsi(self):
        """
        Calculates 3, 5, and 10-minute RSI using pandas-ta.
        Works with UTC timestamps in self.historical_df.
        Returns latest RSI values as a dictionary.
        """
        df = self.historical_df.copy()

        # Ensure 'timestamp' is datetime index
        df = df.set_index('timestamp').sort_index()

        rsi_results = {}

        for timeframe in [ 5, 10]:
            resampled = df.resample(f'{timeframe}min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            # Compute RSI
            rsi = ta.rsi(resampled['close'], length=14)

            if not rsi.dropna().empty:
                latest_rsi = rsi.dropna().iloc[-1]
            else:
                latest_rsi = None

            rsi_results[f'RSI_{timeframe}m'] = latest_rsi

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

    def check_retracement(self):
        """
        Checks for retracement from swing high.
        If retracement is found in a predefined zone, returns trade parameters.
        """
        latest = self.historical_df.iloc[-1]

        # Skip if already processed
        if self.retracement_found:
            return None

        # Calculate retracement
        diff = self.swing_high - self.swing_low
        if diff == 0:
            print("[Error] Swing high and low are equal. Cannot compute retracement.")
            raise "[Error] Swing high and low are equal. Cannot compute retracement."
            return None
        
        # # Calculate retracement based on direction of the swing
        # if self.swing_high > self.swing_low:
        #     # Uptrend → downward retracement
        #     retracement = (self.swing_high - latest['low']) / diff
        # else:
        #     # Downtrend → upward retracement
        #     retracement = (latest['high'] - self.swing_low) / abs(diff)

        low_window = self.historical_df.loc[self.swing_high_index:self.historical_df.index[-1]]
        lowest_low = low_window['low'].min()
        print('min low',lowest_low)
        retracement = (self.swing_high - lowest_low) / diff


        # retracement = (self.swing_high - latest['low']) / diff

        # Check each zone in the retracement matrix
        for zone in RETRACEMENT_MATRIX:
            print(zone)
            print(zone['upper'] < retracement < zone['lower'])
            if zone['upper'] < retracement < zone['lower']:
            # if zone['lower'] < retracement < zone['upper']:
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
            else:
                print('no match found in zone')
        # No retracement found in any zone
        # return None


if __name__ == "__main__":
    hist_data = pd.read_csv('54086.csv')
    # live = pd.read_csv('54094.csv').tail(375)
    obj=OptionsStrikePriceTrader(token='54086', historical_df=hist_data, dhan_user_obj=None)
    print(obj.swing_high , obj.swing_low)
    res=obj.check_retracement()
    open("result.txt", "a").write(str(res) + "\n")
    
        

    
