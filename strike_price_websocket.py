import logging
from dhanhq import DhanContext, MarketFeed
from datetime import datetime, timedelta
import pandas as pd
import pytz
from creds import *
# from options import OptionsStrikePriceTrader
from test import OptionsStrikePriceTrader as OSPT

# Setup timezone
ist = pytz.timezone("Asia/Kolkata")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("trade_log.log"),
        logging.StreamHandler()
    ]
)

def get_previous_minute_candle(df: pd.DataFrame):
    now = datetime.now(ist)
    target_time = now - timedelta(minutes=1)
    target_time = target_time.replace(second=0, microsecond=0)

    if df['timestamp'].dtype == 'O' or df['timestamp'].dt.tz is None:
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('Asia/Kolkata')

    filtered_row = df[df['timestamp'] == target_time]
    return filtered_row.iloc[0] if not filtered_row.empty else None

def load_credentials():
            # Load credentials from a JSON file
            with open('creds.json', 'r') as file:
                creds = json.load(file)
            return creds
user_creds = load_credentials()
user_ids=[2,5]


def trigger(token):
    print(f"Triggering bot for token: {token}")
    admin_obj = UserUtilsClass('7')
    dhan_context = admin_obj.dhan_context
    print('check point 1')
    candle_df = admin_obj.get_strike_price_historical_data(token=token)
    # opt_obj = OptionsStrikePriceTrader(token=token, historical_df=candle_df, dhan_user_obj=admin_obj)
    opt_obj=OSPT(token=token, historical_df=candle_df, dhan_user_obj=admin_obj)
    rsi_value=opt_obj.calculate_multi_rsi()

    print('check point 2')
    # if not(opt_obj.candle_rsi_checker()):
    if rsi_value['RSI_5m'] < 50 and rsi_value['RSI_10m'] < 50:
        return 'initial condition failed'
    # signals_dict=opt_obj.check_fibonacci_entry_signal()
    signals_dict=opt_obj.check_retracement()
    with open("fib.txt", "a") as f:
                    f.write(f"type og fib {type(signals_dict)}  check_fibonacci_entry_signal: {str(signals_dict)}\n")

    entry_triggered = True
    stop_loss = None
    target = None
    # self.dhan_context = DhanContext(user_dict['dhan_creds']['client_id'],user_dict['dhan_creds']['access_token'])
    # self.dhan = dhanhq(self.dhan_context)
    if isinstance(signals_dict, dict) :
                    entry_triggered = True
                    # entry_price = signals_dict['entry_price']
                    # stop_loss = signals_dict['stop_loss']
                    # target = signals_dict['target']

                    entry_price = signals_dict['entry']
                    stop_loss = signals_dict['stop_loss']
                    target = signals_dict['target']

                    logging.info(f"📈 Entry Triggered @ {entry_price} | SL: {stop_loss} | Target: {target}")
                    
                    for user_id in user_ids:
                        user_dict = user_creds[str(user_id)]
                        dhan_context = DhanContext(user_dict['dhan_creds']['client_id'],user_dict['dhan_creds']['access_token'])
                        dhan = dhanhq(dhan_context)
                        dhan.place_order(
                            security_id=token,
                            exchange_segment=admin_obj.dhan.NSE_FNO,
                            transaction_type=admin_obj.dhan.BUY,
                            quantity=30,
                            order_type=admin_obj.dhan.MARKET,
                            product_type=admin_obj.dhan.INTRA,
                            price=0
                        )
    else:
         return f'error in check_fibonacci_entry_signal {signals_dict}'
    print('check point 3')


    instruments = [(MarketFeed.NSE_FNO, token, MarketFeed.Ticker)]
    version = "v2"
    

    import asyncio
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    print('check point 4')

    data = MarketFeed(dhan_context, instruments, version)
    

    flagtime = datetime.now(ist)
    # admin_obj.dhan.place_order(
    #                     security_id=token,
    #                     exchange_segment=admin_obj.dhan.NSE_FNO,
    #                     transaction_type=admin_obj.dhan.BUY,
    #                     quantity=35,
    #                     order_type=admin_obj.dhan.MARKET,
    #                     product_type=admin_obj.dhan.INTRA,
    #                     price=0
    #                 )

    try:
        while True:
            print("🔄 Waiting for data...")
            data.run_forever()
            response = data.get_data()
            if not response or 'LTP' not in response:
                continue

            current_ltp = float(response['LTP'])
            print(current_ltp)

            if not entry_triggered and datetime.now(ist) - flagtime >= timedelta(minutes=1):
                flagtime = datetime.now(ist)

                last_df = admin_obj.get_last_min_candle(token=token)
                if last_df.empty:
                    continue

                row = get_previous_minute_candle(df=last_df.tail(3))
                if row is None:
                    continue

                # opt_obj.add_live_data(
                #     open_=row['open'],
                #     high=row['high'],
                #     low=row['low'],
                #     close=row['close'],
                #     timestamp=row['timestamp'],
                # )
                opt_obj.add_live_data(
                    open_=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    timestamp=row['timestamp'].replace(tzinfo=ist)
                )
                

                # signals_dict = opt_obj.check_fibonacci_entry_signal()

                # if isinstance(signals_dict, dict):
                #     entry_triggered = True
                #     entry_price = signals_dict['entry_price']
                #     stop_loss = signals_dict['stop_loss']
                #     target = signals_dict['target']

                #     logging.info(f"📈 Entry Triggered @ {entry_price} | SL: {stop_loss} | Target: {target}")
                #     admin_obj.dhan.place_order(
                #         security_id=token,
                #         exchange_segment=admin_obj.dhan.NSE_FNO,
                #         transaction_type=admin_obj.dhan.BUY,
                #         quantity=30,
                #         order_type=admin_obj.dhan.MARKET,
                #         product_type=admin_obj.dhan.INTRA,
                #         price=0
                #     )
            current_rsi=opt_obj.calculate_multi_rsi()
            if False and entry_triggered:
                pass
            if  current_ltp <= stop_loss:
                logging.info(f"🛑 Exit: Stop Loss Hit @ {current_ltp}")
                for user_id in user_ids:
                        user_dict = user_creds[str(user_id)]
                        dhan_context = DhanContext(user_dict['dhan_creds']['client_id'],user_dict['dhan_creds']['access_token'])
                        dhan = dhanhq(dhan_context)
                        dhan.place_order(
                            security_id=token,
                            exchange_segment=admin_obj.dhan.NSE_FNO,
                            transaction_type=admin_obj.dhan.BUY,
                            quantity=30,
                            order_type=admin_obj.dhan.MARKET,
                            product_type=admin_obj.dhan.INTRA,
                            price=0
                        )
                # admin_obj.dhan.place_order(
                #     security_id=token,
                #     exchange_segment=admin_obj.dhan.NSE_FNO,
                #     transaction_type=admin_obj.dhan.SELL,
                #     quantity=30,
                #     order_type=admin_obj.dhan.MARKET,
                #     product_type=admin_obj.dhan.INTRA,
                #     price=0
                # )
                return f"🛑 Exit: Stop Loss Hit @ {current_ltp}"
                # break

            elif current_ltp >= target:
                logging.info(f"✅ Exit: Target Hit @ {current_ltp}")
                # t=admin_obj.dhan.place_order(
                #     security_id=token,
                #     exchange_segment=admin_obj.dhan.NSE_FNO,
                #     transaction_type=admin_obj.dhan.SELL,
                #     quantity=30,
                #     order_type=admin_obj.dhan.MARKET,
                #     product_type=admin_obj.dhan.INTRA,
                #     price=0
                # )
                for user_id in user_ids:
                        user_dict = user_creds[str(user_id)]
                        dhan_context = DhanContext(user_dict['dhan_creds']['client_id'],user_dict['dhan_creds']['access_token'])
                        dhan = dhanhq(dhan_context)
                        dhan.place_order(
                            security_id=token,
                            exchange_segment=admin_obj.dhan.NSE_FNO,
                            transaction_type=admin_obj.dhan.BUY,
                            quantity=30,
                            order_type=admin_obj.dhan.MARKET,
                            product_type=admin_obj.dhan.INTRA,
                            price=0
                        )
                return f"✅ Exit: Target Hit @ {current_ltp} "
                # break
            
            # elif opt_obj.candle_rsi_checker():
            elif   current_rsi['RSI_5m'] < 50 or current_rsi['RSI_10m'] < 50:
                logging.info(f"✅ Exit: rsi Hit @ {current_ltp}")
                # admin_obj.dhan.place_order(
                #     security_id=token,
                #     exchange_segment=admin_obj.dhan.NSE_FNO,
                #     transaction_type=admin_obj.dhan.SELL,
                #     quantity=30,
                #     order_type=admin_obj.dhan.MARKET,
                #     product_type=admin_obj.dhan.INTRA,
                #     price=0
                # )
                for user_id in user_ids:
                        user_dict = user_creds[str(user_id)]
                        dhan_context = DhanContext(user_dict['dhan_creds']['client_id'],user_dict['dhan_creds']['access_token'])
                        dhan = dhanhq(dhan_context)
                        dhan.place_order(
                            security_id=token,
                            exchange_segment=admin_obj.dhan.NSE_FNO,
                            transaction_type=admin_obj.dhan.BUY,
                            quantity=30,
                            order_type=admin_obj.dhan.MARKET,
                            product_type=admin_obj.dhan.INTRA,
                            price=0
                        )
                return f"✅ Exit: rsi Hit @ {current_ltp} dhan response {t}"
                    # break

    except Exception as e:
        logging.exception(f"❌ Exception occurred: {e}")
    finally:
        data.close()
        logging.info("🔒 WebSocket connection closed.")


if __name__ == '__main__':
    logging.info("🚀 Triggering bot...")
    x=trigger(token='54033')
    print(x)
