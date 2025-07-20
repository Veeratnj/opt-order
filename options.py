#                                               உ

import pandas as pd
import pandas_ta as ta
import numpy as np

# 1. Define zone matrix as class-level constant
RETRACEMENT_MATRIX = [
    {"lower": 0.884, "upper": 0.854, "entry": 0.702, "sl": 0.884, "target": 0.44},
    {"lower": 0.854, "upper": 0.786, "entry": 0.618, "sl": 0.854, "target": 0.382},
    {"lower": 0.786, "upper": 0.618, "entry": 0.5, "sl": 0.786, "target": 0.236},
    {"lower": 0.618, "upper": 0.5, "entry": 0.44, "sl": 0.702, "target": -0.272},
    {"lower": 0.5, "upper": 0.382, "entry": 0.309, "sl": 0.575, "target": -0.382},
    {"lower": 0.44, "upper": 0.236, "entry": 0.163, "sl": 0.44, "target": -0.618},
    {"lower": 0.236, "upper": 0.146, "entry": 0.073, "sl": 0.309, "target": -1},
]


class OptionsStrikePriceTrader():
    def __init__(self, token, historical_df, dhan_user_obj):
        self.token = token
        self.historical_df = historical_df.copy()  # Ensure safe copy
        self.dhan_user_obj = dhan_user_obj

    def get_fibonacci_levels(self, low, high):
        diff = high - low
        return {
            '0%': high,
            '23.6%': high - 0.236 * diff,
            '38.2%': high - 0.382 * diff,
            '50.0%': high - 0.5 * diff,
            '61.8%': high - 0.618 * diff,
            '78.6%': high - 0.786 * diff,
            '100%': low
        }

    def detect_previous_swing2(self, total_candles=64, groups=8):
        """
        Detect swing low and high by splitting last `total_candles` into `groups`,
        then using the lowest low and highest high from each group to form 'super candles'.

        Returns (swing_low, swing_high) or None.
        """
        if len(self.historical_df) < total_candles + 1:
            return None  # Not enough data

        # Exclude the most recent candle (entry signal)
        df_window = self.historical_df.iloc[-(total_candles + 1):-1]
        group_size = total_candles // groups

        lows = []
        highs = []

        for i in range(0, total_candles, group_size):
            group = df_window.iloc[i:i+group_size]
            lows.append(group['low'].min())
            highs.append(group['high'].max())

        swing_low = min(lows)
        swing_high = max(highs)
        return swing_low, swing_high

    def detect_previous_swing(self, total_candles=64, groups=8):
        """
        Detect previous swing high (most recent), then find the swing low before it.
        
        Process is Right-to-Left: 
        1. Find the most recent swing high.
        2. Then find the swing low BEFORE that high.
        
        Returns:
            tuple: (swing_low, swing_high), or (None, None) if not found.
        """
        if len(self.historical_df) < total_candles + 1:
            return None, None

        df_window = self.historical_df.iloc[-(total_candles + 1):-1]

        # Split into groups right to left (recent to past)
        groups_split = np.array_split(df_window[::-1], groups)

        swing_high = None
        swing_high_group_idx = None

        # Step 1: Find the most recent group with highest high
        for idx, group in enumerate(groups_split):
            group_high = group['high'].max()
            if swing_high is None or group_high > swing_high:
                swing_high = group_high
                swing_high_group_idx = idx

        if swing_high_group_idx is None:
            return None, None

        # Step 2: Search for swing low in groups AFTER the high (since list is reversed, it's BEFORE in actual time)
        swing_low = None
        for group in groups_split[swing_high_group_idx + 1:]:
            group_low = group['low'].min()
            if swing_low is None or group_low < swing_low:
                swing_low = group_low

        if swing_low is None:
            return None, None

        return swing_low, swing_high
    
    def candle_rsi_checker(self):
        df = self.historical_df.copy()

        # Ensure datetime index for resampling
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        # --- 5-minute RSI ---
        df_5min = df.resample('5min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        df_5min['RSI_5min'] = ta.rsi(df_5min['close'], length=14)

        # --- 10-minute RSI ---
        df_10min = df.resample('10min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        df_10min['RSI_10min'] = ta.rsi(df_10min['close'], length=14)

        latest_rsi_5 = df_5min['RSI_5min'].dropna().iloc[-1] if not df_5min['RSI_5min'].dropna().empty else None
        latest_rsi_10 = df_10min['RSI_10min'].dropna().iloc[-1] if not df_10min['RSI_10min'].dropna().empty else None

        if latest_rsi_5 is not None and latest_rsi_10 is not None:
            return latest_rsi_5 > 50 and latest_rsi_10 > 50
        else:
            return False 

    def add_live_data(self, open_, high, low, close, timestamp):
        ts = pd.to_datetime(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize('Asia/Kolkata')
        else:
            ts = ts.tz_convert('Asia/Kolkata')

        # Create new row
        new_row = pd.DataFrame([{
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': None,
            'timestamp': ts
        }])

        self.historical_df = pd.concat([self.historical_df, new_row], ignore_index=True)
        self.historical_df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        self.historical_df.sort_values('timestamp', inplace=True)
        self.historical_df.reset_index(drop=True, inplace=True)

        # Ensure fib columns exist
        fib_cols = ['fib_0%', 'fib_23.6%', 'fib_38.2%', 'fib_50.0%', 'fib_61.8%', 'fib_78.6%', 'fib_100%']
        for col in fib_cols:
            if col not in self.historical_df.columns:
                self.historical_df[col] = None

        # Add fib levels if enough data
        if len(self.historical_df) >= 8:
            swing = self.detect_previous_swing()
            if swing:
                swing_low, swing_high = swing
                fib = self.get_fibonacci_levels(swing_low, swing_high)

                # Assign fib values to the latest row
                for level, value in fib.items():
                    col_name = f'fib_{level}'
                    self.historical_df.at[self.historical_df.index[-1], col_name] = value

        return self.historical_df

    def check_fibonacci_entry_signal(self):
        """
        Use the latest candle to check if entry criteria is met based on custom retracement zones.
        """
        if len(self.historical_df) < 8:
            return None

        latest_index = self.historical_df.index[-1]
        latest = self.historical_df.iloc[-1]
        signal_low = latest['low']
        signal_close = latest['close']

        swing = self.detect_previous_swing()
        if not swing:
            return None

        swing_low, swing_high = swing
        diff = swing_high - swing_low

        for zone in RETRACEMENT_MATRIX:
            # Calculate absolute price levels from percentage
            lower_price = swing_high - zone["lower"] * diff
            upper_price = swing_high - zone["upper"] * diff

            # Entry signal must be between lower and upper retracement levels
            if lower_price < signal_low < upper_price:
                entry_price = swing_high - zone["entry"] * diff
                stop_loss = swing_high - zone["sl"] * diff
                target = swing_high - zone["target"] * diff

                # Set signal column
                if 'signal' not in self.historical_df.columns:
                    self.historical_df['signal'] = None

                self.historical_df.at[latest_index, 'signal'] = 'BUY'

                return {
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "target": target,
                    "zone": zone,
                    "swing": (swing_low, swing_high)
                }

        # No matching zone found
        if 'signal' not in self.historical_df.columns:
            self.historical_df['signal'] = None
        self.historical_df.at[latest_index, 'signal'] = None

        return None

    def add_live_data1(self, open_, high, low, close, timestamp):
        # """Append live data to historical dataframe with proper timestamp handling."""

        # Ensure timestamp is in datetime format and localized
        ts = pd.to_datetime(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize('Asia/Kolkata')
        else:
            ts = ts.tz_convert('Asia/Kolkata')

        # Create a new row as a DataFrame
        new_row = pd.DataFrame([{
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume':None,
            'timestamp': ts
        }])

        # Append and reindex
        self.historical_df = pd.concat([self.historical_df, new_row], ignore_index=True)

        # Optional: keep the dataframe sorted or drop duplicates by timestamp
        self.historical_df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        self.historical_df.sort_values('timestamp', inplace=True)
        self.historical_df.reset_index(drop=True, inplace=True)

        return self.historical_df  

