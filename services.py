import json
import logging
import time
from SmartApi import SmartConnect
import pandas as pd
import pyotp
import numpy as np
import psql
# from creds import *
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    filename='services.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def get_auth(api_key: str, username: str, pwd: str, token: str) -> SmartConnect:
    """Authenticate and return the SmartConnect object."""
    try:
        logging.info("Authenticating with Angel One API...")
        obj = SmartConnect(api_key=api_key)
        data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())

        global AUTH_TOKEN, FEED_TOKEN, refresh_token, res
        AUTH_TOKEN = data['data']['jwtToken']
        FEED_TOKEN = data['data']['feedToken']
        refresh_token = data['data']['refreshToken']
        res = obj.getProfile(refresh_token)

        logging.info("Authentication successful.")
        return obj
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}", exc_info=True)
        raise

def get_profile() -> Dict[str, Any]:
    """Return the profile details."""
    try:
        logging.info("Fetching profile details...")
        return {
            'auth_token': AUTH_TOKEN,
            'feed_token': FEED_TOKEN,
            'refresh_token': refresh_token,
            'res': res,
        }
    except Exception as e:
        logging.error(f"Failed to fetch profile details: {str(e)}", exc_info=True)
        raise

def place_angelone_order(smart_api_obj: SmartConnect, order_details: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Place an order using the Angel One SmartAPI."""
    try:
        logging.info(f"Placing order with details: {json.dumps(order_details, indent=2)}")
        order_response = smart_api_obj.placeOrder(order_details)

        if order_response:
            logging.info(f"Order placement response: {json.dumps(order_response, indent=2)}")
        else:
            logging.warning("Received empty response from placeOrder API.")

        return order_response
    except Exception as e:
        logging.error(f"An error occurred while placing the order: {str(e)}", exc_info=True)
        return None

def get_historical_data1(
    smart_api_obj: SmartConnect, exchange: str, symboltoken: str, interval: str, fromdate: str, todate: str
) -> Optional[pd.DataFrame]:
    """Fetch historical data """
    historic_param = {
            "exchange": exchange,
            "symboltoken": symboltoken,
            "interval": interval,
            "fromdate": fromdate,
            "todate": todate,
        }
    try:
        logging.info(f"Fetching historical data for symboltoken={symboltoken}, interval={interval}")
        historic_param = {
            "exchange": exchange,
            "symboltoken": symboltoken,
            "interval": interval,
            "fromdate": fromdate,
            "todate": todate,
        }
        raw_data = smart_api_obj.getCandleData(historic_param)
        df = pd.DataFrame(raw_data)

        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        data_df = pd.DataFrame(df['data'].tolist(), columns=columns)
        data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])

        # Calculate EMAs
        # data_df['short'] = data_df['close'].ewm(span=5, adjust=False).mean()
        # data_df['middle'] = data_df['close'].ewm(span=21, adjust=False).mean()
        # data_df['long'] = data_df['close'].ewm(span=63, adjust=False).mean()

        # Initialize buy/sell columns
        # data_df['buy'] = np.nan
        # data_df['sell'] = np.nan
        # data_df['buy_exit'] = np.nan
        # data_df['sell_exit'] = np.nan

        data_df.to_csv(f'{symboltoken}.csv', index=False)
        print(f"Historical data saved to {symboltoken}.csv")
        logging.info("Historical data fetched ")
        return data_df
    except Exception as e:
        logging.error(f"An error occurred while fetching historical data: {str(e)}", exc_info=True)
        print(f"An error occurred while fetching historical data: {str(e)}")
        time.sleep(5)
        # get_historical_data(
        #     smart_api_obj, exchange, symboltoken, interval, fromdate, todate
        # )
        return None

def get_historical_data(
    smart_api_obj: SmartConnect,
    exchange: str,
    symboltoken: str,
    interval: str,
    fromdate: str,
    todate: str,
    max_retries: int = 5,
    save_to_csv: bool = True,
    retry_delay: float = 1.5
) -> Optional[pd.DataFrame]:
    """Fetch historical data with retries and rate-limit handling."""

    historic_param = {
        "exchange": exchange,
        "symboltoken": symboltoken,
        "interval": interval,
        "fromdate": fromdate,
        "todate": todate,
    }

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"[{symboltoken}] Attempt {attempt}: Fetching data...")
            raw_data = smart_api_obj.getCandleData(historic_param)

            if not raw_data or 'data' not in raw_data or not raw_data['data']:
                logging.warning(f"[{symboltoken}] No data returned.")
                return None

            df = pd.DataFrame(raw_data['data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            if save_to_csv:
                file_name = f"{symboltoken}.csv"
                # df.to_csv(file_name, index=False)
                logging.info(f"[{symboltoken}] Data saved to {file_name}")
                print(f"✅ Historical data saved to {file_name}")

            return df

        except Exception as e:
            err_msg = str(e)
            logging.error(f"[{symboltoken}] Error on attempt {attempt}: {err_msg}", exc_info=True)

            if "Access denied" in err_msg or "exceeding access rate" in err_msg:
                wait_time = retry_delay * attempt  # exponential backoff
                print(f"⏳ Rate limit hit for {symboltoken}, sleeping {wait_time}s before retrying...")
                time.sleep(wait_time)
            else:
                print(f"❌ Error for {symboltoken}, sleeping {retry_delay}s...")
                time.sleep(retry_delay)

    print(f"❌ Failed to fetch data for {symboltoken} after {max_retries} retries.")
    return None

def buy_sell_function12(data: pd.DataFrame) -> tuple:
    """Generate buy/sell signals based on EMA crossover strategy."""
    try:
        logging.info("Generating buy/sell signals...")
        buy_list, sell_list, buy_exit_list, sell_exit_list = [], [], [], []
        flag_long, flag_short = False, False

        for i in range(len(data)):
            short_temp = data['short'].iloc[i]
            middle_temp = data['middle'].iloc[i]
            long_temp = data['long'].iloc[i]

            if not flag_long and not flag_short and middle_temp < long_temp and short_temp < middle_temp:
                sell_list.append(1)
                buy_list.append(np.nan)
                sell_exit_list.append(np.nan)
                buy_exit_list.append(np.nan)
                flag_short = True
            elif flag_short and short_temp > middle_temp:
                sell_exit_list.append(1)
                buy_list.append(np.nan)
                sell_list.append(np.nan)
                buy_exit_list.append(np.nan)
                flag_short = False
            elif not flag_long and not flag_short and middle_temp < long_temp and short_temp > middle_temp:
                buy_list.append(1)
                sell_list.append(np.nan)
                buy_exit_list.append(np.nan)
                sell_exit_list.append(np.nan)
                flag_long = True
            elif flag_long and short_temp < middle_temp:
                buy_exit_list.append(1)
                sell_list.append(np.nan)
                buy_list.append(np.nan)
                sell_exit_list.append(np.nan)
                flag_long = False
            else:
                buy_list.append(np.nan)
                sell_list.append(np.nan)
                buy_exit_list.append(np.nan)
                sell_exit_list.append(np.nan)


        # for i in range(len(data)):
        #     short = data['short'].iloc[i]
        #     middle = data['middle'].iloc[i]
        #     long = data['long'].iloc[i]

        #     # Skip if any EMA is NaN
        #     if pd.isna(short) or pd.isna(middle) or pd.isna(long):
        #         buy_list.append(np.nan)
        #         sell_list.append(np.nan)
        #         buy_exit_list.append(np.nan)
        #         sell_exit_list.append(np.nan)
        #         continue

        #     # Short Entry
        #     if not flag_short and not flag_long and short < middle and middle < long:
        #         buy_list.append(np.nan)
        #         sell_list.append(1)
        #         buy_exit_list.append(np.nan)
        #         sell_exit_list.append(np.nan)
        #         flag_short = True

        #     # Short Exit
        #     elif flag_short and short > middle:
        #         buy_list.append(np.nan)
        #         sell_list.append(np.nan)
        #         buy_exit_list.append(np.nan)
        #         sell_exit_list.append(1)
        #         flag_short = False

        #     # Long Entry
        #     elif not flag_long and not flag_short and short > middle and middle < long:
        #         buy_list.append(1)
        #         sell_list.append(np.nan)
        #         buy_exit_list.append(np.nan)
        #         sell_exit_list.append(np.nan)
        #         flag_long = True

        #     # Long Exit
        #     elif flag_long and short < middle:
        #         buy_list.append(np.nan)
        #         sell_list.append(np.nan)
        #         buy_exit_list.append(1)
        #         sell_exit_list.append(np.nan)
        #         flag_long = False

        #     else:
        #         buy_list.append(np.nan)
        #         sell_list.append(np.nan)
        #         buy_exit_list.append(np.nan)
        #         sell_exit_list.append(np.nan)

        logging.info("Buy/sell signals generated successfully.")
        return buy_list, sell_list, buy_exit_list, sell_exit_list
    except Exception as e:
        logging.error(f"Error generating buy/sell signals: {str(e)}", exc_info=True)
        raise


def get_latest_ltp_from_db(token: str) -> Optional[Dict[str, Any]]:
    """Fetch the latest LTP from the database."""
    sql = """
    SELECT last_update, ltp
    FROM stock_details
    WHERE token = :token
    LIMIT 1
    """
    try:
        logging.info(f"Fetching latest LTP for token={token} from database...")
        row = psql.execute_query(raw_sql=sql, params={"token": token})
        if row:
            logging.info(f"Latest LTP fetched: {row[0]}")
            return {"timestamp": row[0]['last_update'], "close": row[0]['ltp']}
        else:
            logging.warning(f"No LTP found for token={token}.")
            return None
    except Exception as e:
        logging.error(f"Database error while fetching LTP: {str(e)}", exc_info=True)
        return None

def combine_historical_with_live_algo12(historical_df: pd.DataFrame, token: str) -> pd.DataFrame:
    """Combine historical data with live LTP and recalculate signals."""
    try:
        logging.info(f"Combining historical data with live LTP for token={token}...")
        latest = get_latest_ltp_from_db(token)

        if latest:
            new_row = pd.DataFrame([{
                "timestamp": pd.to_datetime(latest["timestamp"]),
                "open": np.nan,
                "high": np.nan,
                "low": np.nan,
                "close": float(latest["close"]),
                "volume": np.nan,
                "buy": np.nan,
                "sell": np.nan,
                "buy_exit": np.nan,
                "sell_exit": np.nan,
            }])
            combined_df = pd.concat([historical_df, new_row], ignore_index=True)

            # Recalculate EMAs
            combined_df['short'] = combined_df['close'].ewm(span=5, adjust=False).mean()
            combined_df['middle'] = combined_df['close'].ewm(span=21, adjust=False).mean()
            combined_df['long'] = combined_df['close'].ewm(span=63, adjust=False).mean()

            # Generate buy/sell signals
            buy_list, sell_list, buy_exit_list, sell_exit_list = buy_sell_function(combined_df.tail(1))
            combined_df.loc[combined_df.index[-1], 'buy'] = buy_list[0]
            combined_df.loc[combined_df.index[-1], 'sell'] = sell_list[0]
            combined_df.loc[combined_df.index[-1], 'buy_exit'] = buy_exit_list[0]
            combined_df.loc[combined_df.index[-1], 'sell_exit'] = sell_exit_list[0]

            logging.info(f"Historical and live data combined successfully. {str([buy_list, sell_list, buy_exit_list, sell_exit_list])}")
            return combined_df
        else:
            logging.warning("No latest price found. Returning historical data as is.")
            return historical_df
    except Exception as e:
        logging.error(f"Error combining historical and live data: {str(e)}", exc_info=True)
        raise

