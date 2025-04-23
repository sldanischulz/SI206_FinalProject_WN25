# SI 206 Final Project
# Team name:
#   - DOJE
# Team members:
#   - Jack Bernard (jackber@umich.edu - UMID 43241772)
#   - Oleg Korobkov (olegko@umich.edu - UMID 58329022)
#   - Danielle Schulz (dfaria@umich.edu - UMID 63218489)

import http.client
import re
import os
import requests
import sqlite3
from truthbrush.api import Api
from datetime import datetime, timezone, timedelta
import json
from polygon import RESTClient
from datetime import datetime
import jwt
from cryptography.hazmat.primitives import serialization
import time
import secrets

'''
Abstract:
API's:
    - Truthbrush TruthSocial API
    - Polygon Market API
    - Coinbase Crypto Currency API
    - SerpApi Google Trends API

Step by step: (this is for me to check myself and see if I understand what's up)
  FIRST ANALYSIS: The correlation between Trump's posts and the Stock Market  
    1. Pull info from truthbrush API
    2. Understand output
    3. Pull info from market one API
    4. Limit output to 25, figure out how to pull from multiple different dates for both data bases
    5. Determine timeframe for analysis
    6. Join databaframes based on time 
    7. Calculate correlation
    8. Plot maps

  SECOND ANALYSIS  
    1. Break posts data into a word count dictionary
    2. Run top something words into Google Trends API
    3. Identify the posts that contain the trendiest words
    4. Compare those posts and stock numbers with the ones with least trendy words
    5. Plot the results
    6. Write up the analysis and conclusions
    7. Boot
    8. Rally
'''

# Historical Stock Market Data API Request
class Polygon():
    """Initializes the Polygon API client with the API key."""
    
    def __init__(self):
        """Inputs: self
        Output: None
        Desc: Initializes Polygon API with API key provided in file"""

        path = os.path.dirname(os.path.abspath(__file__))
        full_path = path + r'\polygon_api_key.txt'
        with open(full_path, 'r') as file:
            self.key = file.read().strip()
        pass
    
    def get_stonks(self, date_start, date_end):
        """Inputs: self, start date (str in in yyy-mm-dd format), end_date (str in yyy-mm-dd format)
            Output: Creates json file of historical stock market data for the NASDAQ indice. Will provide open, high, low, and close data inside the json.
            Desc: Initializes Polygon API with API key provided in file"""
        
        client = RESTClient(self.key)

        aggs = []
        for a in client.list_aggs(
            "I:NDX",
            1,
            "day",
            date_start,
            date_end,
            adjusted="true",
            sort="asc",
            limit=120,
        ):
            print(a)
            aggs.append(a)

        print(aggs)

        new_aggs_dict = {}
        
        for a in aggs:
            timestamp_ms = a.timestamp
            timestamp_sec = timestamp_ms / 1000

            readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
            #print("REAAAAAD", readable_date)
            new_aggs_dict[str(readable_date)] = {
            "open": a.open,
            "high": a.high,
            "low": a.low,
            "close": a.close,
            "volume": a.volume,
            "vwap": a.vwap,
            "timestamp": a.timestamp,
            "transactions": a.transactions,
            "otc": a.otc,
            }
            
        with open(f"stocks_{str(date_start)[0:10]}.json", "w") as json_file:
            json.dump(new_aggs_dict, json_file, indent=4)
        return (f"stocks_{str(date_start)[0:10]}.json")



def get_token(request_path="/api/v3/brokerage/products/BTC-USD", request_method="GET", request_host="api.coinbase.com"):
    """Inputs: request_path (str, optional), request_method (str, optional), request_host (str, optional)
        Output: returns JWT token generated with API token
        Desc: Uses defined Coinbase API key to make a token request on the provided path, and return that token for use in other functions.
        If path, method and host are not defined, defaults to needed values to get token for Coinbase Candles."""

    key_name       = "organizations/32eb8db2-c903-4e05-ad8f-364aaba57abc/apiKeys/a3d644d7-e3b9-415d-8fd0-6e21b134075a"
    key_secret     = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIDVtwQe7UrJs6CLIPUnTnO7yaGZe0ApStBdNt1CfgtZkoAoGCCqGSM49\nAwEHoUQDQgAE8NCwJj9td0GBnvZfjGasrjxjC2pHlSM4hafan+ThKJqsYCga9tvS\nsHn5X77vJTNi9xKr2kD38Nhu2Y9TSRXpCQ==\n-----END EC PRIVATE KEY-----\n"
    
    private_key_bytes = key_secret.encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None)
    jwt_payload = {
        'sub': key_name,
        'iss': "cdp",
        'nbf': int(time.time()),
        'exp': int(time.time()) + 120,
        'uri': f"{request_method} {request_host}{request_path}",
    }
    jwt_token = jwt.encode(
        jwt_payload,
        private_key,
        algorithm='ES256',
        headers={'kid': key_name, 'nonce': secrets.token_hex()},
    )
    return jwt_token

def coin_candles(coin, start_date, end_date):
    """Inputs: coin (str), start_date (datetime), end_date (datetime)
    Output: Creates json file of historical cryptocurrency market data for the coin specified. Will provide open, high, low, and close data inside the json.
    Desc: Initializes Coinbase API with token provided from get_token, and makes request. Outputs data into json and saves."""

    start_timestamp = start_date.timestamp()
    end_timestamp = end_date.timestamp()

    base = f'/api/v3/brokerage/products/{coin}/candles'

    token = get_token(base)

    conn = http.client.HTTPSConnection("api.coinbase.com")
    payload = ''
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}',

    }
    conn.request("GET", f"{base}?start={int(start_timestamp)}&end={int(end_timestamp)}&granularity=ONE_DAY&limit=25", payload, headers)
    res = conn.getresponse()
    data = res.read()
    decoded_data = data.decode("utf-8")
    with open(f"crypto_{str(start_date)[0:10]}.json", "w") as json_file:
        json.dump(json.loads(decoded_data), json_file, indent=4)
    
    return (f"crypto_{str(start_date)[0:10]}.json")

def get_stonks_finage(stock, date_start, date_end):
    """Inputs: stock (str), start_date (str in in yyy-mm-dd format), end_date (str in in yyy-mm-dd format)
    Output: Creates json file of historical stock ticker data for the stock specified. Stock should be in ticker format (Ex: NVDA) Will provide open, high, low, and close data inside the json.
    Desc: Initializes Finage API with key provided, and makes request. Outputs data into json and saves."""\
    
    conn = http.client.HTTPSConnection("api.finage.co.uk")
    payload = ''
    headers = {}
    conn.request("GET", f"/agg/stock/{stock}/1/day/{date_start}/{date_end}?apikey=API_KEY35CA0PQ4F85418304JYBS65LB01AWMNG", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data)
    decoded_data = data.decode("utf-8")
    with open(f"{stock}_{date_start}.json", "w") as json_file:
        json.dump(json.loads(decoded_data), json_file, indent=4)
    return (f"{stock}_{date_start}.json")


def get_json_content(filename):

    '''
    opens file file, loads content as json object

    ARGUMENTS: 
        filename: name of file to be opened

    RETURNS: 
        json dictionary OR an empty dict if the file could not be opened 
    '''

    try:
        with open(filename, 'r', encoding="utf-8") as file:
            data = json.load(file)
        return data
    
    except:
        empty = {}
        return empty


def set_up_database(db_name):

    """
    Sets up a SQLite database connection and cursor.

    Parameters
    -----------------------
    db_name: str
        The name of the SQLite database

    Returns
    -----------------------
    Tuple (cursor, connection):
        A tuple containing the database cursor and connection objects.
    """

    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn



def set_up_market_coin_table(cur, conn):
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Bitcoin (id INTEGER PRIMARY KEY, timestamp TEXT, date TEXT, open INTEGER, close INTEGER, high INTEGER, low INTEGER)"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS Nvidia (id INTEGER PRIMARY KEY, timestamp TEXT, date TEXT, open INTEGER, close INTEGER, high INTEGER, low INTEGER)"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS Nasdaq (id INTEGER PRIMARY KEY, timestamp TEXT, date TEXT, open INTEGER, close INTEGER, high INTEGER, low INTEGER)"
    )

    conn.commit()


def add_criptodata_to_table(coin, cur, conn):
    '''
    Data = List of dictionaries with market data  
    '''
    for i in range(len(coin['candles'])):
        timestamp_sec = int(coin['candles'][i]['start'])
        readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        temp = str(readable_date)[0:10]
        str_date = temp.replace("-", "")
        num_date = int(str_date)

        cur.execute(
            "INSERT OR IGNORE INTO Bitcoin (id, timestamp, date, open, close, high, low) VALUES (?,?,?,?,?,?,?)", (num_date,
                                                                                                          coin['candles'][i]['start'],
                                                                                                          str(readable_date),
                                                                                                          coin['candles'][i]['open'],
                                                                                                          coin['candles'][i]['close'],
                                                                                                          coin['candles'][i]['high'],
                                                                                                          coin['candles'][i]['low'])                                                                                                        
        )

    conn.commit()


def add_nvdadata_to_table(coin, cur, conn):
    '''
    Data = List of dictionaries with market data  
    '''
    for i in range(len(coin['results'])):
        timestamp_ms = int(coin['results'][i]['t'])
        timestamp_sec = timestamp_ms / 1000
        readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        temp = str(readable_date)[0:10]
        str_date = temp.replace("-", "")
        num_date = int(str_date)

        cur.execute(
            "INSERT OR IGNORE INTO Nvidia (id, timestamp, date, open, close, high, low) VALUES (?,?,?,?,?,?,?)", (num_date,
                                                                                                            coin['results'][i]['t'],
                                                                                                            str(readable_date),
                                                                                                            coin['results'][i]['o'],
                                                                                                            coin['results'][i]['c'],
                                                                                                            coin['results'][i]['h'],
                                                                                                            coin['results'][i]['l'])                                                                                                        
        )
    conn.commit()

def add_stockdata_to_table(coin, cur, conn):
    '''
    Data = List of dictionaries with market data  
    '''
    for i in coin.values():
        timestamp_ms = int(i['timestamp'])
        timestamp_sec = timestamp_ms / 1000
        readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        temp = str(readable_date)[0:10]
        str_date = temp.replace("-", "")
        num_date = int(str_date)
        cur.execute(
            "INSERT OR IGNORE INTO Nasdaq (id, timestamp, date, open, close, high, low) VALUES (?,?,?,?,?,?,?)", (num_date,
                                                                                                      i['timestamp'],
                                                                                                      str(readable_date),
                                                                                                      i['open'],
                                                                                                      i['close'],
                                                                                                      i['high'],
                                                                                                      i['low'])                                                                                                        
        )
     
    conn.commit()


def menu_apis():
    print("Welcome to our SI 206 Final Project!\n \n We hope you enjoy it!\n")
    status = False
    while status == False:
        print("You have selected to pull data from APIs.")
        inp = input("Select an API to pull data from:\n   [1] NASDAQ via Polygon\n   [2] Bitcoin via Coinbase\n   [3] NVIDIA via Finage\n ")
        if inp == "1":
            status = True
            print("Selected: NASDAQ")
            return 'NASDAQ'
        elif inp == "2":
            status = True
            print("Selected: Bitcoin")
            return 'Bitcoin'
        elif inp == "3":
            status = True
            print("Selected: NVIDIA")
            return 'NVIDIA'
        elif inp == "4":
            status = True
            print("!!!ENTERING SECRET BONUS LEVEL!!!")
            return "secret"
        else:
            print("Invalid input. Please try again.")
            continue
    
def get_dates():
    print("Let's collect some data!")

    status = False
    while status == False:
        try:
            offset = timezone(timedelta(hours=2))

            s_year = int(input("Please enter the year you would like to begin pulling data from (YYYY): "))
            s_month = int(input("Please enter the month you would like to begin pulling data from (MM): "))
            s_day = int(input("Please enter the day you would like to begin pulling data from (DD): "))
            e_year = int(input("Please enter the year you would like to end pulling data from (YYYY): "))
            e_month = int(input("Please enter the month you would like to end pulling data from (MM): "))
            e_day = int(input("Please enter the day you would like to end pulling data from (DD): "))

            start_date = datetime(s_year, s_month, s_day, tzinfo=offset)
            end_date = datetime(e_year, e_month, e_day, tzinfo=offset)
            now = datetime.now(tz=offset)

            if start_date > end_date:
                print("Start date must be before end date.")
                continue
            elif end_date > now:
                print("End date must be before today.")
                continue
            elif datetime(e_year, e_month, e_day, tzinfo=offset) - datetime(s_year, s_month, s_day, tzinfo=offset) > timedelta(days=25):
                end_date = datetime(s_year, s_month, s_day, tzinfo=offset) + timedelta(days=25)
                print("Date interval set to 25 days.")
                status = True

            return start_date, end_date
        
        except:
            print("Invalid input. Please try again.")
            return None

def secret(conn,cur):
    offset = timezone(timedelta(hours=2))
    p = Polygon()
    p.get_stonks(str(datetime(2024, 11, 1, tzinfo=offset))[0:10], str(datetime(2024, 11, 26, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2024, 11, 1, tzinfo=offset))[0:10], str(datetime(2024, 11, 26, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2024, 11, 1, tzinfo=offset), datetime(2024, 11, 26, tzinfo=offset))
    time.sleep(10)

    p.get_stonks(str(datetime(2024, 11, 26, tzinfo=offset))[0:10], str(datetime(2024, 12, 21, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2024, 11, 26, tzinfo=offset))[0:10], str(datetime(2024, 12, 21, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2024, 11, 26, tzinfo=offset), datetime(2024, 12, 21, tzinfo=offset))
    time.sleep(10)

    p.get_stonks(str(datetime(2024, 12, 21, tzinfo=offset))[0:10], str(datetime(2025, 1, 15, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2024, 12, 21, tzinfo=offset))[0:10], str(datetime(2025, 1, 15, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2024, 12, 21, tzinfo=offset), datetime(2025, 1, 15, tzinfo=offset))
    time.sleep(10)

    p.get_stonks(str(datetime(2025, 1, 15, tzinfo=offset))[0:10], str(datetime(2025, 2, 9, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2025, 1, 15, tzinfo=offset))[0:10], str(datetime(2025, 2, 9, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2025, 1, 15, tzinfo=offset), datetime(2025, 2, 9, tzinfo=offset))
    time.sleep(10)

    p.get_stonks(str(datetime(2025, 2, 9, tzinfo=offset))[0:10], str(datetime(2025, 3, 6, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2025, 2, 9, tzinfo=offset))[0:10], str(datetime(2025, 3, 6, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2025, 2, 9, tzinfo=offset), datetime(2025, 3, 6, tzinfo=offset))
    time.sleep(10)

    p.get_stonks(str(datetime(2025, 3, 6, tzinfo=offset))[0:10], str(datetime(2025, 3, 31, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2025, 3, 6, tzinfo=offset))[0:10], str(datetime(2025, 3, 31, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2025, 3, 6, tzinfo=offset), datetime(2025, 3, 31, tzinfo=offset))
    time.sleep(10)

    p.get_stonks(str(datetime(2025, 3, 31, tzinfo=offset))[0:10], str(datetime(2025, 4, 17, tzinfo=offset))[0:10])
    get_stonks_finage("NVDA", str(datetime(2025, 3, 31, tzinfo=offset))[0:10], str(datetime(2025, 4, 17, tzinfo=offset))[0:10])
    coin_candles("BTC-USD", datetime(2025, 3, 31, tzinfo=offset), datetime(2025, 4, 17, tzinfo=offset))
    time.sleep(10)

        
    polygon_list = ["stocks_2024-11-01.json",      # 01
                    "stocks_2024-11-26.json",      # 01
                    "stocks_2024-12-21.json",      # 02
                    "stocks_2025-01-15.json",      # 03
                    "stocks_2025-02-09.json",
                    "stocks_2025-03-06.json",
                    "stocks_2025-03-31.json"]

    finage_list = ["NVDA_2024-11-01.json",      # 01
                    "NVDA_2024-11-26.json",      # 01
                    "NVDA_2024-12-21.json",      # 02
                    "NVDA_2025-01-15.json",      # 03
                    "NVDA_2025-02-09.json",
                    "NVDA_2025-03-06.json",
                    "NVDA_2025-03-31.json"]         # 04
  
    cripto_list = ["crypto_2024-11-01.json",       # 01
                   "crypto_2024-11-26.json",       # 02
                   "crypto_2024-12-21.json",       # 03
                   "crypto_2025-01-15.json",       # 02
                   "crypto_2025-02-09.json",       # 03
                   "crypto_2025-03-06.json",
                   "crypto_2025-03-31.json"]       # 04 

    for i in range(7):

        # Pulls data from json and make it into list of dictionaries
        polygon = get_json_content(polygon_list[i])
        finage = get_json_content(finage_list[i])
        cripto = get_json_content(cripto_list[i])

        add_nvdadata_to_table(finage, cur, conn)
        add_stockdata_to_table(polygon, cur, conn)
        add_criptodata_to_table(cripto, cur, conn)

    conn.commit()
    
def main():

    ######################################################################### COLLECTING DATA
    api = menu_apis()
    
    ######################################################################### SETTING UP DATABASE
    database_name = "final_project.db"
    cur, conn = set_up_database(database_name) 

    ######################################################################### SETTING UP TABLES
    ## Creates tables 
    # set_up_posts_tables(cur, conn)
    set_up_market_coin_table(cur, conn)

    start_date, end_date = get_dates()
    #print("START", start_date, "END", end_date)
    
    offset = timezone(timedelta(hours=2))

    if api == 'NASDAQ':
        p = Polygon()
        ndq_json = p.get_stonks(str(start_date)[0:10], str(end_date)[0:10])
        
    elif api == 'Bitcoin':
        bit_json = coin_candles("BTC-USD", start_date, end_date)
    
    elif api == 'NVIDIA':
        nvd_json = get_stonks_finage("NVDA", str(start_date)[0:10], str(end_date)[0:10])
    elif api == 'secret':
        secret(conn,cur)

    print("\nData has been collected.\nNow adding it into the database.\n")


    ######################################################################### ADDING DATA TO TABLES

    if api == 'NASDAQ':
        data = get_json_content(ndq_json)
        print("Data added:", len(data.keys()))
        add_stockdata_to_table(data, cur, conn)
    
    elif api == 'Bitcoin':
        data = get_json_content(bit_json)
        print("Data added:", len(data["candles"]))
        add_criptodata_to_table(data, cur, conn)

    elif api == 'NVIDIA':
        data = get_json_content(nvd_json)
        print("Data added:", len(data['results']))
        add_nvdadata_to_table(data, cur, conn)

    print("Done adding data to the database\n")

    
    ######################################################################### CLOSING NOTES AND COUNTING ITEMS IN TABLES

    cur.execute("SELECT COUNT(id) FROM Nasdaq")
    nasdaq_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(id) FROM Nvidia")
    nvidia_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(id) FROM Bitcoin")
    bitcoin_count = cur.fetchone()[0]

    print("Number of items in Nasdaq table:", nasdaq_count)
    print("Number of items in Nvidia table:", nvidia_count)
    print("Number of items in Bitcoin table:", bitcoin_count)

    # pass


if __name__ == '__main__':
    main()
    