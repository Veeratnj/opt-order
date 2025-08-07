import logging
import time
from threading import Thread
import pandas as pd
from uuid import uuid4
import pytz
from sqlalchemy import text, bindparam
import psql
from services import (
    get_profile,
    place_angelone_order,
    get_auth,
    get_historical_data,
    buy_sell_function12
)
from creds import *
from datetime import datetime, timedelta,time as time_c
from typing import Dict, Any, List

from strike_price_websocket import trigger
ist = pytz.timezone("Asia/Kolkata")

# from strategies import TripleEMAStrategyOptimized
from fullcode_ import TripleEMAStrategyOptimized

class StrategyTrader:
    def __init__(self):
        # self.smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
        pass

    def place_order(self,order_params: Dict[str, Any], user_id: int, stock_token: str,smart_api_obj) -> None:
        """Helper function to place an order."""
        try:
            logging.info(f"Placing order with params: {order_params}")
            result = place_angelone_order(
                # smart_api_obj=get_auth(api_key=api_key, username=username, pwd=pwd, token=token),
                order_details=order_params,
                smart_api_obj=smart_api_obj,
            )
            logging.info(f"Order placed successfully for user_id={user_id}, stock_token={stock_token} order id {result}")
            return result
        except Exception as e:
            logging.error(f"Failed to place order for user_id={user_id}, stock_token={stock_token} - {str(e)}", exc_info=True)
            return None

    def get_ohlc1(self, token, time_frame):
        '''
        Get the latest OHLC candle for a given token.
        Note: time_frame is not yet used.
        '''
        query = text('SELECT * FROM ohlc_data WHERE token = :token ORDER BY id DESC, start_time DESC LIMIT 1')
        ohlc_rows = psql.execute_query(raw_sql=query, params={"token": token})

        if not ohlc_rows:
            return None  # or raise an exception if you prefer

        ohlc_row = ohlc_rows[0]

        # Assuming ohlc_row is a tuple in this exact column order:
        # id, token, start_time, open, high, low, close, interval, created_at
        # print(ohlc_row)
        _, _, start_time, open_price, high_price, low_price, close_price, _, _ = list(ohlc_row.values())

        return start_time, open_price, high_price, low_price, close_price


    def get_ohlc2(self,df: pd.DataFrame):
        print(df)
        result=df.tail(1).to_dict(orient='records')[0]
        print(result)
        time.sleep(1)
        
        return result['timestamp'],result['open'],result['high'],result['low'],result['close']

    def get_ohlc(self, token,limit=1, time_frame=None):
        '''
        Get the latest OHLC candle for a given token.
        Note: time_frame is not yet used.
        '''
        query = text('SELECT * FROM bank_nifty_ohlc_data WHERE token = :token ORDER BY id DESC, start_time DESC LIMIT :limit')
        ohlc_rows = psql.execute_query(raw_sql=query, params={"token": token,'limit': limit})

        if not ohlc_rows:
            return None  # or raise an exception if you prefer

        ohlc_row = ohlc_rows[0]

        # Assuming ohlc_row is a tuple in this exact column order:
        # id, token, start_time, open, high, low, close, interval, created_at
        # print(ohlc_row)
        _, _, start_time, open_price, high_price, low_price, close_price, _, _ = list(ohlc_row.values())

        return start_time, float(open_price), float(high_price), float(low_price), float(close_price)

    def get_historical_ohlc(self, token, limit=500, time_frame=None):
        """
        Get historical OHLC data for a given token and return as Pandas DataFrame.

        Args:
            token (str): Instrument token.
            limit (int): Max number of OHLC rows to fetch (default=400).
            time_frame (str): Optional, for future timeframe support.

        Returns:
            pd.DataFrame: Columns - ['start_time', 'open', 'high', 'low', 'close']
        """
        query = text('''
            SELECT * FROM (
                SELECT * FROM bank_nifty_ohlc_data 
                WHERE token = :token 
                ORDER BY start_time DESC, id DESC 
                LIMIT :limit
            ) sub
            ORDER BY  id ASC
        ''')
        
        ohlc_rows = psql.execute_query(raw_sql=query, params={"token": token, 'limit': limit})

        if not ohlc_rows:
            return pd.DataFrame(columns=['start_time', 'open', 'high', 'low', 'close'])

        df = pd.DataFrame(ohlc_rows)

        # Only required columns
        df = df[['start_time', 'open', 'high', 'low', 'close']].copy()
        
        # Ensure correct types (optional if DB always returns floats)
        df = df.astype({
            'open': float,
            'high': float,
            'low': float,
            'close': float
        })

        return df

    # def get_live_ohlc(self,smart_api_obj,token:str,from_date:str='',to_date:str='',interval:str='FIVE_MINUTE',) -> pd.DataFrame:
       
    #     df=smart_api_obj.get_historical_data_(
    #             # smart_api_obj=self.smart_api_obj,
    #             exchange="NSE",
    #             symboltoken=token,
    #             interval="FIVE_MINUTE",
    #             from_date=from_date,
    #             to_date=to_date
    #         )

        
    
    
    def fetch_from_db(self,query: str, params: Dict[str, Any], error_message: str) -> Dict[str, Any]:
        """Helper function to fetch data from the database."""
        try:
            result = psql.execute_query(query, params=params)
            if not result:
                logging.error(error_message)
                raise ValueError(error_message)
            return result[0]
        except Exception as e:
            logging.error(f"Database query failed: {error_message} - {str(e)}", exc_info=True)
            raise

    def get_latest_ltp(self, stock_token: str='99926009'):
        """
        Fetch the latest LTP and timestamp for the given stock_token from the database.
        Returns (timestamp, ltp)
        """
        try:
            query = """
                SELECT last_update, ltp
                FROM stock_details
                WHERE token = :stock_token
                ORDER BY last_update DESC
                LIMIT 1
            """
            params = {"stock_token": stock_token}
            result = psql.execute_query(query, params=params)
            if not result:
                error_message = f"No LTP found for stock_token: {stock_token}"
                logging.error(error_message)
                raise ValueError(error_message)
            row = result[0]
            # Ensure timestamp is in datetime format
            ltp_timestamp = row['last_update']
            ltp_price = row['ltp']
            return ltp_timestamp, ltp_price
        except Exception as e:
            logging.error(f"Database query failed for LTP: {str(e)}", exc_info=True)
            raise

    def get_stock_trend_type(self, stock_token: str):
        """
        fetch the stock type is bullish or bearish
        """
        try:
            query = """
                SELECT trend_type
                FROM stocks
                WHERE token = :stock_token
                LIMIT 1
            """
            params = {"stock_token": stock_token}
            result = psql.execute_query(query, params=params)
            if not result:
                error_message = f"No trend_type for stock_token: {stock_token}"
                logging.error(error_message)
                raise ValueError(error_message)
            row = result[0]
            # Ensure timestamp is in datetime format
            
            trend_result = row['trend_type']
            return trend_result
        except Exception as e:
            logging.error(f"Database query failed for get trend type: {str(e)}", exc_info=True)
            raise

    
    def is_market_open(self):
        """Returns True if current time is within trading hours (e.g., 9:15 to 15:30). Adjust as needed."""
        now = datetime.now()
        market_open = now.replace(hour=9, minute=20, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=25, second=0, microsecond=0)
        # return True
        return market_open <= now <= market_close

    def trade_function(self, row: Dict[str, Any],api_obj) -> None:
        try:
            logging.info(f"Starting trade_function for row: {row}")
            def stocks_quantity(ltp: float, balance: float ,user_id:int=0) -> int:
                logging.info(f"Starting stocks_quantity  ltp::{ltp},  balance::{balance}")
                if user_id==7:
                    return (10000/ltp)*4
                
                if float(ltp) <= 0:
                    return 0
                    # raise ValueError("LTP must be greater than 0")
                usable_balance = float(balance) * 0.65
                logging.info(f"quantity {int(usable_balance // float(ltp))}")
                return int(usable_balance // float(ltp))

            # quantity = row['quantity']
            quantity = 2
            stock_token = row['stock_token']
            trade_count = row['trade_count']
            user_id = row['user_id']
            strategy_id = row['strategy_id']
            user_strategy_id = row['id']

            stock_details = self.fetch_from_db(
                "SELECT * FROM stock_details WHERE token = :stock_token",
                {'stock_token': str(stock_token)},
                f"Stock details not found for stock_token: {stock_token}"
            )

            order_manager_uuid = str(uuid4())
            # 
            # --- Initialize historical data and strategy ---
            # smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
            today = datetime.now()
            fromdate = (today - timedelta(days=10)).strftime("%Y-%m-%d %H:%M")
            todate = today.strftime("%Y-%m-%d %H:%M")
            print(f"Fetching historical data from {fromdate} to {todate}")
            
            # historical_df = api_obj.get_nifty_fifty_historical()
            historical_df = self.get_historical_ohlc(token='25',limit=500)
            if historical_df is None or historical_df.empty:
                logging.error("No historical data found, aborting trade_function.")
                return

            strategy = TripleEMAStrategyOptimized(token=stock_token,)
            strategy.load_historical_data(historical_df)

            def is_time_window():
                now = datetime.now()
                # Adjust this logic for your timeframe (5min, 1min, 30sec, etc.)
                return now.minute % 1 == 0 and now.second < 3
            open_order=False
            
            previous_candle_time=None
            stop_loss = None
            target = None
            previous_entry_exit_key = None
            order_params=None
            while (trade_count > 0 and True) or open_order:
                

                exit_flag=False
                if previous_entry_exit_key is not None and stop_loss is not None and target is not None:
                    if previous_entry_exit_key == 'BUY_EXIT':
                        if ltp_price<=stop_loss or ltp_price>=target or datetime.now().time() >= time_c(14,25):
                            exit_flag=True
                            print('exit flag is true')
                            logging.info(f"buy exit ltp_price={ltp_price} stop_loss={stop_loss} target={target} previous_entry_exit_key={previous_entry_exit_key} stock_token={stock_token} cond1{ltp_price<=stop_loss} cond2{ltp_price>=target}")
                    elif previous_entry_exit_key == 'SELL_EXIT':
                        if ltp_price>=stop_loss or ltp_price<=target or datetime.now().time() >= time_c(14,25):
                            exit_flag=True
                            print('exit flag is true')
                            logging.info(f"sell exit ltp_price={ltp_price} stop_loss={stop_loss} target={target} previous_entry_exit_key={previous_entry_exit_key} stock_token={stock_token} cond1{ltp_price>=stop_loss} cond2{ltp_price<=target}")

                    # if stop_loss<=ltp_price or target>=ltp_price:
                    #     exit_flag=True
                    #     print('exit flag is true')
                # while not is_time_window() and self.is_market_open():
                #     time.sleep(0.5)
                if not self.is_market_open():
                    # logging.info("Market closed. Exiting trade_function.")
                    continue

                try:
                    ltp_price=9999999999999999999999
                    pass
                    # ltp_timestamp, ltp_price = self.get_latest_ltp()
                except Exception as e:
                    print(e)
                    logging.error(f"Failed to fetch latest LTP: {e}")
                    # time.sleep(2)
                    continue

                # signal = strategy.add_live_price(ltp_timestamp, ltp_price)
                # open_, high, low, close, end_time = candles_builder(stock_token, )
                # temp_df=api_obj.get_nifty_fifty_historical().tail(1)
                # print(temp_df)
                start_time,open_, high, low, close = self.get_ohlc(token='25',limit=1)
                # start_time,open_, high, low, close = smart_api_obj.get_latest_5min_candle(symboltoken=stock_token, )
                if start_time == previous_candle_time and not exit_flag:
                    previous_candle_time=start_time
                    continue
                previous_candle_time=start_time
                print('start time is ==',start_time)
                strategy.add_live_data(open_=open_,close=close, high=high, low=low, volume=0, timestamp=start_time)
                signal,stop_loss_,target_,strike_price = strategy.generate_signal()
                if target_ is not None:
                    target = target_
                if stop_loss_ is not None:
                    stop_loss = stop_loss_
                
                logging.info(f"Signal generated: {signal} stop loss {stop_loss} target {target} previous entry exit key {previous_entry_exit_key} ltp {ltp_price}  token {stock_token} ")

                if signal == 'BUY_ENTRY' and datetime.now().time()<=time_c(13, 30):
                    previous_entry_exit_key = 'BUY_EXIT'
                    quantity=stocks_quantity(ltp=ltp_price,balance=api_obj.smart_api_obj.rmsLimit()['data']['availablecash'],user_id=user_id)
                    tokens_data_frame = pd.read_excel('options-aug-2025.xlsx')
                    # tokens_data_frame.drop_duplicates(subset=['strike_price'],inplace=True)
                    # option_token_row = tokens_data_frame[tokens_data_frame['strike_price'] == strike_price and tokens_data_frame['position']=='CE']
                    option_token_row = tokens_data_frame[
                        (tokens_data_frame['strike_price'] == int(strike_price)) & 
                        (tokens_data_frame['position'] == 'CE')
                    ]
                    # option_token_row['token']
                    print('length is :: ',len(tokens_data_frame),"strike price is ::",strike_price)
                    print(f"BUY_ENTRY signal received token number is {option_token_row['token'].iloc[0]}")
                    open_order=True
                    trade_count -= 1
                    logging.info(f"strike price token number is {str(option_token_row['token'].iloc[0])}")
                    # symbol=str(option_token_row['index_name'].iloc[0])+' '+str(option_token_row['strike_price'].iloc[0])
                    symbol=option_token_row['symbol']
                    res=trigger(token=str(option_token_row['token'].iloc[0]),position='CE',symbol=symbol)
                    print('trigger response',res)
                    logging.info(f"trigger response  : {res}")



                elif signal == 'SELL_ENTRY' and datetime.now().time() <= time_c(13, 30): 
                    previous_entry_exit_key = 'SELL_EXIT'
                    quantity=stocks_quantity(ltp=ltp_price,balance=api_obj.smart_api_obj.rmsLimit()['data']['availablecash'])
                    print('SELL_ENTRY signal received')
                    tokens_data_frame = pd.read_excel('options-aug-2025.xlsx')
                    print('length is :: ',len(tokens_data_frame),"strike price is ::",strike_price)
                    # tokens_data_frame.drop_duplicates(subset=['strike_price'],inplace=True)
                    # option_token_row = tokens_data_frame[tokens_data_frame['strike_price'] == strike_price and tokens_data_frame['position']=='PE']  
                    option_token_row = tokens_data_frame[
                        (tokens_data_frame['strike_price'] == int(strike_price)) & 
                        (tokens_data_frame['position'] == 'PE')
                    ]                  
                    print(f"SELL_ENTRY signal received token number is {option_token_row['token'].iloc[0]}")
                    open_order=True
                    trade_count -= 1
                    logging.info(f"strike price token number is {str(option_token_row['token'].iloc[0])}")
                    # symbol=str(option_token_row['index_name'].iloc[0])+' '+str(option_token_row['strike_price'].iloc[0])
                    symbol=option_token_row['symbol']
                    res=trigger(token=str(option_token_row['token'].iloc[0]),position='PE',symbol=symbol)
                    print('trigger response',res)
                    logging.info(f"trigger response  : {res}")
                    
                    
                    open_order=True
                    
                    trade_count -= 1
                    
                   
                # elif signal == 'BUY_EXIT' or (previous_entry_exit_key == 'BUY_EXIT' and exit_flag and False):
                #     open_order=False
                #     # psql.execute_query(
                #     #     """
                #     #     UPDATE equity_trade_history
                #     #     SET exit_ltp = :exit_ltp, 
                #     #         trade_exit_time = :trade_exit_time,
                #     #         total_price = :total_price
                #     #     WHERE order_id = :order_id AND trade_type = 'buy' AND exit_ltp = 0;
                #     #     """,
                #     #     params={
                #     #         "exit_ltp": ltp_price,
                #     #         "trade_exit_time": datetime.now(),
                #     #         "order_id": order_manager_uuid,
                #     #         "total_price": quantity * ltp_price
                #     #     }
                #     # )
                #     # # psql.execute_query(
                #     #     raw_sql="""
                #     #     UPDATE user_active_strategy
                #     #     SET status = 'close'
                #     #     WHERE id = :id;
                #     #     """,
                #     #     params={"id": row['id']}
                #     # )
                #     # logging.info(f"Buy exit executed for stock_token={stock_token}")
                #     # order_params['transactiontype'] = 'SELL'
                #     # # angelone_response = smart_api_obj.place_order(order_params=order_params, user_id=user_id, stock_token=stock_token,smart_api_obj=smart_api_obj)
                #     # angelone_response = api_obj.place_order(order_params=order_params,)

                # elif False and signal == 'SELL_EXIT' or (previous_entry_exit_key == 'SELL_EXIT' and exit_flag):
                #     print('sell exit signal received')
                #     open_order=False
                #     order_params['transactiontype'] = 'BUY'
                #     psql.execute_query(
                #         """
                #         UPDATE equity_trade_history
                #         SET exit_ltp = :exit_ltp, 
                #             trade_exit_time = :trade_exit_time,
                #             total_price = :total_price
                #         WHERE order_id = :order_id AND trade_type = 'sell' AND exit_ltp = 0;
                #         """,
                #         params={
                #             "exit_ltp": ltp_price,
                #             "trade_exit_time": datetime.now(),
                #             "order_id": order_manager_uuid,
                #             "total_price": quantity * ltp_price
                #         }
                #     )
                #     psql.execute_query(
                #         raw_sql="""
                #         UPDATE user_active_strategy
                #         SET status = 'close'
                #         WHERE id = :id;
                #         """,
                #         params={"id": row['id']}
                #     )
                #     logging.info(f"Sell exit executed for stock_token={stock_token}")
                #     # angelone_response = smart_api_obj.place_order(order_params=order_params, user_id=user_id, stock_token=stock_token,smart_api_obj=smart_api_obj)
                #     angelone_response =api_obj.place_order(order_params=order_params,)

                # Sleep to avoid double execution in the same minute
                time.sleep(2)


            print((trade_count > 0 and self.is_market_open()) or open_order)
            print(trade_count)
            print(self.is_market_open())
            print(open_order)
            if trade_count > 0:
                logging.info("Trading day ended before all trades could be completed.")

        except Exception as e:
            # raise e
            logging.error(f"Error processing trade for user_id={row.get('user_id')} - {str(e)}", exc_info=True)

    def main(self) -> None:
        """Main function to start the trading process (no threading, for testing)."""
        try:
            from datetime import date 
            to_day_date=date.today().strftime("%Y-%m-%d")
            print('hi')
            
            print('1234567890')
            data=[{'id': 3064, 'user_id': 7, 
                   'strategy_id': '123qwe', 
                   'stock_token': '25', 
                   'trade_count': 1, 
                   'quantity': 2, 
                   'paper_trade': True, 
                   'is_active': True, 
                   'is_started': False, 
                   'deactivated_at': None, 
                   'deactivated_by': 0, 
                   'status': 'pending'}]
            
            

            
            api_obj_dicts={}
            print('qwe')
            for id in [{'id': 7}]:
                print(f"User ID: {id['id']}")
                api_obj_dicts[id['id']] = UserUtilsClass(user_id=str(id['id']))
            print('user utils ready')



            for row in data:
                # print(row['stock_token'])
                # continue
                # sql = text("UPDATE user_active_strategy SET is_started = true WHERE id = :id")
                # psql.execute_query(sql, params={"id": row['id']})
                print(f"Updated is_started=true for ID: {row['id']}")
               
                # t = Thread(target=self.trade_function, args=(row,api_obj_dicts[row['user_id']]))
                # t.start()
                self.trade_function(row,api_obj_dicts[row['user_id']])
                print(api_obj_dicts[row['user_id']])
                print(f"Starting thread for user_id={row['user_id']}, strategy_id={row['strategy_id']}, token={row['stock_token']}")
                time.sleep(1)
        except Exception as e:
            logging.error("Error in run method", exc_info=True)


if __name__ == "__main__":
    trader = StrategyTrader()
    print('hello')
    trader.main()