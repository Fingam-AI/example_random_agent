import json
import random
import requests
from typing import Tuple


class FingamAPIOperations:
    def get_auth_access_token(self, username: str, password: str) -> str:
        url = f"https://cognito-idp.ap-northeast-1.amazonaws.com/"

        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth"
        }

        payload = {
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": "cognito-public-client-id",
            "AuthParameters": {
                "USERNAME": username,
                "PASSWORD": password
            }
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload))

        tokens = response.json()["AuthenticationResult"]
        access_token = tokens["AccessToken"]

        return access_token

    def request_get_status(self, access_token: str, api_key: str, symbol: str = None) -> dict:
        api_gateway_url = "https://fingam.ai/paper/getstatus"

        if symbol:
            api_gateway_url = f"{api_gateway_url}?symbol={symbol}"

        api_headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "Authorization": f"Bearer {access_token}"
        }

        api_response = requests.get(api_gateway_url, headers=api_headers)

        return api_response.json()

    def request_set_leverage(self, access_token: str, api_key: str, symbol: str, leverage: int) -> dict:
        api_gateway_url = "https://fingam.ai/paper/setleverage"

        api_headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "Authorization": f"Bearer {access_token}"
        }

        payload = {
            "symbol": symbol,
            "leverage": leverage
        }

        api_response = requests.post(api_gateway_url, headers=api_headers, json={"payload": payload})

        return api_response.json()

    def request_order(self, access_token: str, api_key: str, symbol: str, side: str, size: float = None) -> dict:
        api_gateway_url = "https://fingam.ai/paper/order"

        api_headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "Authorization": f"Bearer {access_token}"
        }

        payload = {
            "symbol": symbol,
            "side": side,
            "size": size
        }

        api_response = requests.post(api_gateway_url, headers=api_headers, json={"payload": payload})

        return api_response.json()


class AIAgent:
    def __init__(self):
        self.TRADE_COIN_AMOUNTS = {
            "BTCUSDT": 0.01,
            "ETHUSDT": 1,
            "SOLUSDT": 1
        }

    def get_next_size_and_side_for_symbol(self, symbol_status_data: dict) -> Tuple[float, str]:
        coin_symbol = symbol_status_data.get("symbol")
        current_size = symbol_status_data.get("size")
        current_side = symbol_status_data.get("side")

        ai_decision = random.choice(["increase_position", "decrease_position", "close_position"])

        # Calculate next_size based on decision
        trade_amount = self.TRADE_COIN_AMOUNTS.get(coin_symbol, 0)

        if ai_decision == "increase_position":
            next_size = round(abs(current_size) + trade_amount, 8)
        elif ai_decision == "decrease_position":
            next_size = round(abs(current_size) - trade_amount, 8)
        else:
            next_size = 0

        # Decide next_side
        if next_size == 0:
            next_side = "CLOSE"
        else:
            if current_side == "CLOSE" or current_size == 0:
                next_side = "LONG" if next_size > 0 else "SHORT"
            else:
                next_side = current_side

        return abs(next_size), next_side


# API constants
USERNAME = "example_username@fingam.ai"
PASSWORD = "ExamplePassword"
API_KEY = "ExampleAPIKey"

# Trade constants
TRADE_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


if __name__ == '__main__':
    fingam = FingamAPIOperations()
    ai_agent = AIAgent()

    access_token = fingam.get_auth_access_token(username=USERNAME, password=PASSWORD)

    for symbol in TRADE_SYMBOLS:
        print(f"Trading symbol: {symbol}")
        symbol_status = fingam.request_get_status(access_token=access_token, api_key=API_KEY, symbol=symbol)

        predicted_next_size, predicted_next_side = ai_agent.get_next_size_and_side_for_symbol(symbol_status_data=symbol_status.get("data"))
        print(f"Predicted next size: {predicted_next_size}, Predicted next side: {predicted_next_side}")

        response_data = fingam.request_order(access_token=access_token, api_key=API_KEY, symbol=symbol, side=predicted_next_side, size=predicted_next_size)
        print(json.dumps(response_data, indent=2))

    overall_status = fingam.request_get_status(access_token=access_token, api_key=API_KEY)
    print(f"Overall status: {overall_status}")
