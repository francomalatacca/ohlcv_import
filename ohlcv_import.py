from decouple import config
from binance.client import Client
import json
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import os
from influxdb import InfluxDBClient
import hashlib
import socket
from numpy import nan as NaN
import pytz
import dateutil.parser

DATABASE_NAME = os.getenv('DATABASE_NAME', 'cryptos')
DEFAULT_SYMBOL = os.getenv('DEFAULT_SYMBOL', 'ADAEUR')
EXCHANGE_DOMAIN = os.getenv('EXCHANGE_DOMAIN', 'binance.com')
FILE_CACHE_MAX_TIME_MIN = os.getenv('FILE_CACHE_MAX_TIME_MIN', 1)
BITCOIN_EPOCH_DATE = '2008-01-09T00:00:50.000000+00:00'


def main():
    API_KEY = config('APIKey')
    API_SECRET = config('APISecret')

    INFLUX_USER = config('INFLUX_USER')
    INFLUX_PASSWORD = config('INFLUX_USER')

    client = Client(API_KEY, API_SECRET)
    exchange_info = get_exchange_info(client)
    print_exchage_info(exchange_info)

    ohlcv = get_historical(client)
    ohlcv_df = pd.DataFrame(ohlcv)
    ohlcv_df.columns = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'Quote_asset_volume',
                        'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'null']

    push_data(INFLUX_USER, INFLUX_PASSWORD, DEFAULT_SYMBOL,
              ohlcv_df, exchange_info, 'PROD', True)


def push_data(username, password, symbol, measurements, exchange_info, environment='DEV', write_points=True, exchange_domain=EXCHANGE_DOMAIN):
    client = InfluxDBClient(host='192.168.1.100', port=8086,
                            username=username, password=password)
    client.switch_database(DATABASE_NAME)

    timezone = pytz.timezone(exchange_info["timezone"])
    results = client.query(
        f'SELECT * FROM ADAEUR ORDER BY time desc LIMIT 1')
    results.get_points(tags={'environment': environment, 'exchange': exchange_domain})
    lastEntryTime = dateutil.parser.isoparse(BITCOIN_EPOCH_DATE)
    for result in results:

        lastEntryTime =  dateutil.parser.isoparse(result[0]['time'])
        print(f'*** LAST ENTRY FOUND {result[0]["measurementId"]}@{result[0]["time"]}')
    hostname = socket.gethostname()
    entries = []

    skipped = 0
    for i, measurement in measurements.iterrows():

        measurement_string = symbol + '@' + exchange_domain + ':' + \
            str(measurement['Open_time']) + ',' + \
            str(measurement['Close_time'])
        measurement_id = hashlib.sha1(
            measurement_string.encode('utf-8')).hexdigest()
        diff = (datetime.fromtimestamp(measurement['Close_time']/1000, timezone) - lastEntryTime).seconds + ((datetime.fromtimestamp(measurement['Close_time']/1000, timezone) - lastEntryTime).days * 24 * 60 * 60) 
        if diff > 59:
            entries.append(
                {
                    "measurement": DEFAULT_SYMBOL,
                    "tags": {
                        "hostname": hostname,
                        "measurementId": measurement_id,
                        "environment": environment,
                        "exchange": exchange_domain,
                        "exchangeTime": datetime.fromtimestamp(round(int(exchange_info["serverTime"]/1000), 0), timezone).isoformat(),
                        "exchangeTimeZone": exchange_info["timezone"]
                    },
                    "time": datetime.fromtimestamp(round(int(measurement["Close_time"]/1000), 0), timezone).isoformat(),
                    "fields": {
                        "ot": datetime.fromtimestamp(round(int(measurement["Open_time"]/1000), 0), timezone).isoformat(),
                        "o": float(measurement['Open']),
                        "h": float(measurement['High']),
                        "l": float(measurement['Low']),
                        "v": float(measurement['Close']),
                        "c": float(measurement['Volume']),
                        "ct": datetime.fromtimestamp(round(int(measurement["Close_time"]/1000), 0), timezone).isoformat(),
                    }
                }
            )
            print(f'{len(entries)} - pushing {measurement_id}@{datetime.fromtimestamp(round(int(measurement["Close_time"]/1000), 0), timezone).isoformat()} created {diff} seconds after last record')
        else:
            skipped = skipped + 1        
            print(f'*** skip record with diff {diff} sec' )
    if write_points:
        print(f'pushing {len(entries)}, skipped {skipped}')
        with open('_tmp.json', 'w') as f:
            f.write(json.dumps(entries))
        ret = client.write_points(entries)
        client.close()
        print(ret)
    else:
        print(entries)

def get_exchange_info(client, exchange_info_file_name='exchange_info.json'):
    if is_data_cached(exchange_info_file_name):
        print("*** read from cache")
        with open(exchange_info_file_name, 'r') as f:
            content = json.load(f)
            return content

    exchange_info = client.get_exchange_info()
    exchange_info_string = json.dumps(exchange_info, sort_keys=True, indent=4)
    with open(exchange_info_file_name, 'w') as exchange_info_file:
        exchange_info_file.write(exchange_info_string)
    return exchange_info

def print_exchage_info(exchange_info):
    timezone = pytz.timezone(exchange_info["timezone"])
    dt_object = datetime.fromtimestamp(
        exchange_info["serverTime"]/1000, timezone).isoformat()
    print(
        f'server time: {dt_object}\ntimezone: {exchange_info["timezone"]}\nsymbols: {len(exchange_info["symbols"])}')

def get_historical(client, symbol=DEFAULT_SYMBOL, exchange_historical_file_name='historical_file_{symbol}.json'):
    exchange_historical_file_name = exchange_historical_file_name.replace(
        '{symbol}', symbol)

    if not is_data_cached(exchange_historical_file_name):
        with open(exchange_historical_file_name, 'w') as f:
            klines = client.get_historical_klines_generator(
                symbol, Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC")
            klines_dump = json.dumps(list(klines), sort_keys=True, indent=4)
            f.write(klines_dump)

    with open(exchange_historical_file_name, 'r') as f:
        content = json.load(f)
        return content

def get_book(client):
    for kline in client.get_historical_klines_generator("ADAEUR", Client.KLINE_INTERVAL_1MINUTE, "1 day ago UTC"):
        print(kline)

def is_data_cached(filename):
    if not os.path.isfile(filename):
        return False
    today = datetime.now()
    createDate = datetime.fromtimestamp(os.path.getctime(filename))
    print(f'found a {filename} created {createDate} ({(today - createDate).seconds / 60 } min ago - use cache: {(today - createDate).seconds / 60 <= FILE_CACHE_MAX_TIME_MIN})')
    return (today - createDate).seconds / 60 <= FILE_CACHE_MAX_TIME_MIN

if __name__ == "__main__":
    main()
