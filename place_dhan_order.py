import json
from dhanhq import dhanhq, DhanContext  # Assuming correct import

def load_credentials(path='creds.json'):
    """Load credentials from a JSON file."""
    with open(path, 'r') as file:
        return json.load(file)

user_creds = load_credentials()
user_ids = [2] 

def order_function(transaction_type: str, token: str):
    """
    Places an order for each user_id in user_ids.

    Args:
        transaction_type (str): 'entry' for BUY, anything else for SELL
        token (str): Security token (e.g., Dhan token like 54096)

    Returns:
        list: List of responses from Dhan's place_order call
    """
    results = []

    for user_id in user_ids:
        user_dict = user_creds[str(user_id)]
        dhan_context = DhanContext(
            user_dict['dhan_creds']['client_id'],
            user_dict['dhan_creds']['access_token']
        )
        dhan = dhanhq(dhan_context)

        

        try:
            res = dhan.place_order(security_id=token,            
                    exchange_segment=dhan.NSE_FNO,
                    transaction_type=dhan.BUY if transaction_type.lower() == 'entry' else dhan.SELL,
                    quantity=35,
                    order_type=dhan.MARKET,
                    product_type=dhan.INTRA,
                    price=0,
                    )
            open('dhan_response.txt','a').write(f"{str({'user_id': user_id, 'status': 'success', 'response': res})} \n")
            results.append({'user_id': user_id, 'status': 'success', 'response': res})
        except Exception as e:
            open('dhan_response.txt','a').write(f"{'user_id': user_id, 'status': 'failed', 'error': str(e)}\n")
            results.append({'user_id': user_id, 'status': 'failed', 'error': str(e)})

    return results

if __name__ == '__main__':
    response = order_function(transaction_type='entry', token='54033')
    print(json.dumps(response, indent=2))
