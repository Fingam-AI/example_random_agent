from typing import Dict, List, Tuple
from operator import itemgetter
import requests
import pandas as pd
import time
import datetime

CONNECTION_ABORTED_RETRY_AFTER_SECONDS = 10
MILLISECONDS_IN_SECOND = 1000
api_klines_futures_url = "https://fapi.binance.com/fapi/v1/klines"
kline_data_columns = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "number_of_trades",
					  "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"]

def _order_params(data: Dict) -> List[Tuple[str, str]]:
	"""Convert params to list with signature as last element"""
	data = dict(filter(lambda el: el[1] is not None, data.items()))
	has_signature = False
	params = []

	for key, value in data.items():
		if key == 'signature':
			has_signature = True
		else:
			params.append((key, str(value)))

	# sort parameters by key
	params.sort(key=itemgetter(0))
	if has_signature:
		params.append(('signature', data['signature']))
	return params

def _create_ordered_query_string(data: Dict):
	ordered_data = _order_params(data)
	query_string = '&'.join([f"{d[0]}={d[1]}" for d in ordered_data])
	return query_string

def _order_params(data: Dict) -> List[Tuple[str, str]]:
	"""Convert params to list with signature as last element"""
	data = dict(filter(lambda el: el[1] is not None, data.items()))
	has_signature = False
	params = []

	for key, value in data.items():
		if key == 'signature':
			has_signature = True
		else:
			params.append((key, str(value)))

	# sort parameters by key
	params.sort(key=itemgetter(0))
	if has_signature:
		params.append(('signature', data['signature']))
	return params

def get_kline_data_with_interval(symbol, start_time, end_time, interval):
	retry_count = 0
	max_retry_count = 100
	while retry_count <= max_retry_count:
		try:
			data = {
				"symbol": symbol,
				"interval": "{}".format(interval),
				"limit": 1500,
				"startTime": start_time,
				"endTime": end_time
			}

			params = _create_ordered_query_string(data)
			response = requests.get(api_klines_futures_url, params=params)

			if response.status_code == requests.codes.OK:
				df = pd.DataFrame(response.json(), columns=kline_data_columns)
				df = df.apply(pd.to_numeric)
				return df
			elif response.status_code == 504 or response.status_code == 503 or response.status_code == 451:
				print('There is a server side issue, going to retry getting data again. Response status code: {}'.format(response.status_code))
				time.sleep(1)
				retry_count += 1
				continue
			elif response.status_code == 429:
				print("Reached api call limit. Going to retry after {} seconds.".format(int(response.headers["Retry-After"])))
				time.sleep(int(response.headers["Retry-After"]))
				retry_count += 1
				continue
			elif response.status_code == 403:
				print('WAF Limit (Web Application Firewall) has been violated.')
			else:
				print('Binance data retrieval response status is not OK. {}'.format(response))
		except (ConnectionResetError, requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
			print("There has been too many api calls or server side issue. Going to retry after {} seconds.".format(CONNECTION_ABORTED_RETRY_AFTER_SECONDS))
			print(e)
			time.sleep(CONNECTION_ABORTED_RETRY_AFTER_SECONDS)
			retry_count += 1
			continue
	raise Exception("Failed to retrieve data within {} tries.".format(max_retry_count))

if __name__ == '__main__':
	end_datetime = datetime.datetime.now(tz=datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)
	start_datetime = end_datetime - datetime.timedelta(minutes=10000)
	start_timestamp = int(start_datetime.timestamp() * MILLISECONDS_IN_SECOND)
	end_timestamp = int(end_datetime.timestamp() * MILLISECONDS_IN_SECOND) + 1
	print(get_kline_data_with_interval("BTCUSDT", start_timestamp, end_timestamp, "1h")) # "1d" for daily data