import requests

def insert_or_update_ltp(option_symbol: str, ltp: float):
    """
    Calls the /insert-or-update-ltp API with the given option_symbol and ltp.
    """
    url = "https://smartelitetradingclub.live/option/insert-or-update-ltp" 
    
    payload = {
        "option_symbol": option_symbol,  # Matches OptionsLTPRequest.option_symbol
        "ltp": ltp                        # Matches OptionsLTPRequest.ltp
    }

    try:
        response = requests.post(url, json=payload, timeout=1)
        response.raise_for_status()  # Raise exception if HTTP status is 4xx/5xx
        return response.json()
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return None

# Example usage
if __name__ == "__main__":
    result = insert_or_update_ltp("BANKNIFTY24AUG45000CE", 123.45)
    print(result)
