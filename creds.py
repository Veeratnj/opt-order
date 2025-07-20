# veera 4
# api_key = 'Uo3GNqh4'
# username = 'V158268'
# pwd = 1212
# token = "W4UZV4CVSU65J4RMFRCFPMMBEM"


# #raja 2
# api_key = 'um1iKprH'
# username = 'M55065663'
# pwd = 7384
# token = "BO6RSHEAKZGO4DCJYMK2NMGVS4"


# #kamal raj 5
# api_key = 'R77xVFTM'
# username = 'CHNA2756'
# pwd = 3115
# token = "HIPF3HUP467XMWTXZEJROVDOAU"

# #Hemalatha 6
# api_key = '5afTALj7'
# username = 'H59296493'
# pwd = 8689
# token = "C7GPZVQB376G3RHN6IQD6QKJ5A"


# Name             -      Kamal Raj
# Email id          -      kamal0to3@gmail.com
# API Key           -     R77xVFTM
# User Name     -      CHNA2756
# Password        -      3115
# Token             -       
# Totp               -        HIPF3HUP467XMWTXZEJROVDOAU
# Secret Key      -       3ccb6c4a-2b0e-4380-9dec-8bf9af9d2ddf


# Name             -      Hemalatha R
# Email id          -      hemalatha_08krishnan@yahoo.in
# API Key           -      5afTALj7
# User Name     -      H59296493
# Password        -      8689
# Token             -       
# Totp               -        C7GPZVQB376G3RHN6IQD6QKJ5A
# Secret Key      -       8f6cb757-492b-4254-ad2d-ce4c127ba054 


import collections
import json

import pandas as pd
from services import get_auth,get_historical_data,place_angelone_order
from datetime import datetime, timedelta
from dhanhq import dhanhq,DhanContext

def load_credentials():
    # Load credentials from a JSON file
    with open('creds.json', 'r') as file:
        creds = json.load(file)
    return creds

user_creds=load_credentials()



class UserUtilsClass:
    def __init__(self, user_id:str):
        def load_credentials():
            # Load credentials from a JSON file
            with open('creds.json', 'r') as file:
                creds = json.load(file)
            return creds
        user_creds = load_credentials()
        
        # print(user_dict)
        # if not user_dict:
        if str(user_id) not in user_creds:
            print(f"No credentials found for user ID: {user_id}")
            pass
            # raise ValueError(f"No credentials found for user ID: {user_id}")
        else:
            user_dict = user_creds[str(user_id)]
            self.api_key = user_dict["angelone_creds"]["api_key"]
            self.username = user_dict["angelone_creds"]["username"]
            self.pwd = user_dict["angelone_creds"]["pwd"]
            self.token = user_dict["angelone_creds"]["token"]
            self.email = user_dict["angelone_creds"]["email"]
            self.name = user_dict["angelone_creds"]["name"]
            self.smart_api_obj = get_auth(self.api_key, self.username, self.pwd, self.token)
            self.historical_dict = collections.defaultdict(dict)
            self.dhan_context = DhanContext(user_dict['dhan_creds']['client_id'],user_dict['dhan_creds']['access_token'])
            self.dhan = dhanhq(self.dhan_context)

    def get_historical_data_(self,symboltoken="",exchange="",from_date="2025-05-01 08:11", to_date="2025-05-15 08:11", interval="day"):
        
        df=get_historical_data(
            smart_api_obj=self.smart_api_obj,
                exchange=exchange,
                symboltoken=symboltoken,
                interval="FIVE_MINUTE",
                # fromdate='2025-05-15 08:11',
                fromdate=from_date,
                # todate='2025-05-26 08:11'
                todate=to_date
        )
        # self.historical_dict[symboltoken] = df
        return df
        # print(f"Historical data for {symboltoken} from {from_date} to {to_date} fetched successfully.")

    def get_latest_5min_candle(self, symboltoken: str, exchange: str = "NSE") -> dict:
        now = datetime.now()

        # Calculate previous closed 5-minute candle start and end times
        minute = (now.minute // 5) * 5
        end_time = now.replace(minute=minute, second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=5)

        from_date = start_time.strftime('%Y-%m-%d %H:%M')
        to_date = end_time.strftime('%Y-%m-%d %H:%M')
        # print(f"Fetching data for {symboltoken} from {from_date} to {to_date}")
        # from_date = '2025-06-13 13:50'
        # to_date = '2025-06-13 13:55'

        df = self.get_historical_data_(
            symboltoken=symboltoken,
            exchange=exchange,
            from_date=from_date,
            to_date=from_date,
            interval="FIVE_MINUTE"
        )

        if df is None or df.empty:
            print(f"[!] No data found for {from_date} to {to_date}")
            return {}
        
        
        latest_candle = df[df['timestamp'] == from_date]
        row=latest_candle[0]
        if row.empty:
            # print(f"No candle found for {candle_start_time}")
            return None

        row = row.iloc[0]
        return row['timestamp'], row['open'], row['high'], row['low'], row['close']

        # return latest_candle
    
    def place_order(self, order_params: dict):
        
        result=place_angelone_order(
            smart_api_obj=self.smart_api_obj,
            order_details=order_params
        )
        return result
    
    def dhan_get_profile(self):
        return self.dhan.get_fund_limits()
    
    def get_nifty_fifty_historical(self):
        today = datetime.today()
        from_date = (today - timedelta(days=10)).strftime('%Y-%m-%d')
        to_date = today.strftime('%Y-%m-%d')

        data = self.dhan.intraday_minute_data(
            security_id='25',
            exchange_segment='IDX_I',
            instrument_type='INDEX',
            from_date=from_date,
            to_date=to_date,
        )

        # print("Raw response:", data)

        if not data or 'data' not in data or not isinstance(data['data'], dict):
            print("Invalid or missing 'data'")
            return pd.DataFrame()

        df = pd.DataFrame(data['data'])

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')


        return df

    def stocks_quantity(self,ltp: float, balance: float ,user_id:int=0) -> int:
        if user_id==7:
            return (10000/ltp)*4
        
        if float(ltp) <= 0:
            return 0
            # raise ValueError("LTP must be greater than 0")
        usable_balance = float(balance) * 0.65
        return int(usable_balance // float(ltp))
        # return int(float(balance) // float(ltp))

    def get_nse_quantity(self,ltp,user_id) ->int:
        try:
            # angelone_balance = self.smart_api_obj.rmsLimit()['data']['availablecash']
            # print(self.dhan.get_fund_limits())
            dhan_balance = int(self.dhan.get_fund_limits()['data']['availabelBalance'])
            return self.stocks_quantity(ltp=ltp,user_id=user_id,balance=dhan_balance)
        except Exception as e:
            print(e)
            print(self.dhan.get_fund_limits())
            return 5

    def get_dhan_historical_data(self,exchange=dhanhq.INDEX,security_id='25'):
        data=self.dhan.ohlc_data(
            securities={
                "NSE_EQ":[security_id]
            }
        )
        return data

        
    def dhan_order_placement(self,order_params):
        self.dhan.place_order(
                security_id=order_params['security_id'],  
                exchange_segment=order_params['exchange_segment'],
                transaction_type=order_params['transaction_type'],
                order_type=order_params['order_type'],
                product_type=order_params["product_type"],
                quantity=order_params['quantity'],
                price=order_params['price']  
            )

    def __str__(self):
        return f"UserCredentials(api_key={self.api_key}, username={self.username}, pwd={self.pwd}, token={self.token},"

    def get_nifty_fifty_ltp_chart(self,token:str):
        today = datetime.today()
        from_date = (today - timedelta(days=10)).strftime('%Y-%m-%d')
        to_date = today.strftime('%Y-%m-%d')

        data = self.dhan.intraday_minute_data(
            security_id=token,
            exchange_segment='NSE_FNO',
            instrument_type='OPTIDX',
            from_date=from_date,
            to_date=to_date,
        )

        # print("Raw response:", data)

        if not data or 'data' not in data or not isinstance(data['data'], dict):
            print("Invalid or missing 'data'")
            return pd.DataFrame()

        df = pd.DataFrame(data['data'])

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')


        return df

    def get_strike_price_historical_data(self,token:str):
        today = datetime.today()
        from_date = (today - timedelta(days=10)).strftime('%Y-%m-%d')
        to_date = today.strftime('%Y-%m-%d')

        data = self.dhan.intraday_minute_data(
            security_id=token,
            exchange_segment='NSE_FNO',
            instrument_type='OPTIDX',
            from_date=from_date,
            to_date=to_date,
        )
        

        # print("Raw response:", data)

        if not data or 'data' not in data or not isinstance(data['data'], dict):
            print("Invalid or missing 'data'")
            return pd.DataFrame()

        df = pd.DataFrame(data['data'])

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        return df

    def get_last_min_candle(self,token):
        today = datetime.today()
        from_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        to_date = today.strftime('%Y-%m-%d')

        data = self.dhan.intraday_minute_data(
            security_id=token,
            exchange_segment='NSE_FNO',
            instrument_type='OPTIDX',
            from_date=from_date,
            to_date=to_date,
        )
        if not data or 'data' not in data or not isinstance(data['data'], dict):
            print("Invalid or missing 'data'")
            return pd.DataFrame()

        df = pd.DataFrame(data['data'])

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        return df





# print(SmartAPIUserCredentialsClass(user_id=6))