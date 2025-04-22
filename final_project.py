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
#import requests
import json
import sqlite3
#from truthbrush.api import Api
from datetime import datetime, timezone, timedelta
import json
#from polygon import RESTClient
from datetime import datetime
#import jwt
#from cryptography.hazmat.primitives import serialization
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

# # Truth Social API Request
# def truth_user_lookup(api, handle):
#     """Pulls a user's basic information from Truthsocial and stores it in a .json"""

#     user = api.lookup(handle)
#     with open(f"{handle}_info.json", "w") as json_file:
#         json.dump(user, json_file, indent=4)
#     return user

# def truth_pull_posts(api, handle, start_date):
#     """Pulls a user's posts after a defined date from Truthsocial and stores it in a .json"""
#     partial_pull = []
#     # partial_pull = list(api.pull_statuses(username=handle, replies=False, created_after=start_date, verbose=True))
#     try:
#         for a in (api.pull_statuses(username=handle, replies=False, created_after=start_date, verbose=True)):
#             # print(a)
#             partial_pull.append(a)
            
#     except:
#         print(f"Truthbrush is done early before the date. The last post date is {partial_pull[-1]['created_at'][0:10]}")

#     # print(partial_pull)
#     with open(f"{str(start_date)[0:9]}_statuses.json", "w") as json_file:
#         json.dump(partial_pull, json_file, indent=4)
#     return partial_pull

# Historical Stock Market Data API Request
class Polygon():
    """Initializes the Polygon API client with the API key."""
    def __init__(self):
        # Read the API key from a file
        path = os.path.dirname(os.path.abspath(__file__))
        full_path = path + r'\polygon_api_key.txt'
        with open(full_path, 'r') as file:
            self.key = file.read().strip()
        pass
    
    def get_stonks(self, date_start, date_end):
        """Pulls historical NASDAQ Indicie data from Polygon API and stores it in a .json file."""
        
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
        return



def get_token(request_path="/api/v3/brokerage/products/BTC-USD", request_method="GET", request_host="api.coinbase.com"):
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
    
    start_timestamp = start_date.timestamp()
    end_timestamp = end_date.timestamp()
    print(start_timestamp)
    print(end_timestamp)
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


def get_stonks_finage(stock, date_start, date_end):
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


def set_up_posts_tables(cur, conn):
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Posts (post_id INTEGER PRIMARY KEY, timestamp TEXT, post_content TEXT, engagement INTEGER, day_key INTEGER)"
    )
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Engagement (post_id INTEGER PRIMARY KEY, replies INTEGER, reblogs INTEGER, favourites INTEGER)"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS PostPerDay (day_key INTEGER PRIMARY KEY, count INTEGER)"
    )
    
    conn.commit()

def add_posts_to_table(data, cur, conn):
    '''
    Data = List of dictionaries with post data  
    '''
    

    for i in range(len(data)):
        eng = 0
        rep = data[i]['replies_count']
        reb = data[i]['reblogs_count']
        fav = data[i]['favourites_count']
        eng = rep + reb + fav

        cur.execute(
            "INSERT OR IGNORE INTO Engagement (post_id, replies, reblogs, favourites) VALUES (?,?,?,?)",
            (data[i]['id'], rep, reb, fav)
        )

        cur.execute(
            "INSERT OR IGNORE INTO Posts (post_id, timestamp, post_content, engagement) VALUES (?,?,?,?)", (data[i]['id'],
                                                                                                            data[i]['created_at'],
                                                                                                            data[i]['content'],
                                                                                                            eng)                                                                                                        
        )

    conn.commit()

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

# def add_marketdata_to_table(market, cur, conn, m_counter):
#     '''
#     Data = List of dictionaries with market data  
#     '''

#     for key in market.keys():

#         cur.execute(
#             "INSERT OR IGNORE INTO Market (id, timestamp, open, close, high, low) VALUES (?,?,?,?,?,?)", (m_counter,
#                                                                                                           market[key]['timestamp'],
#                                                                                                           market[key]['open'],
#                                                                                                           market[key]['close'],
#                                                                                                           market[key]['high'],
#                                                                                                           market[key]['low'])                                                                                                        
#         )

#         m_counter += 1

#     conn.commit()

#     return m_counter


def add_criptodata_to_table(coin, cur, conn, counter):
    '''
    Data = List of dictionaries with market data  
    '''
    for i in range(len(coin['candles'])):
        timestamp_sec = int(coin['candles'][i]['start'])
        readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)

        cur.execute(
            "INSERT OR IGNORE INTO Bitcoin (id, timestamp, date, open, close, high, low) VALUES (?,?,?,?,?,?,?)", (counter,
                                                                                                          coin['candles'][i]['start'],
                                                                                                          str(readable_date),
                                                                                                          coin['candles'][i]['open'],
                                                                                                          coin['candles'][i]['close'],
                                                                                                          coin['candles'][i]['high'],
                                                                                                          coin['candles'][i]['low'])                                                                                                        
        )
        counter += 1
        
    conn.commit()

    return counter

def add_nvdadata_to_table(coin, cur, conn, counter):
    '''
    Data = List of dictionaries with market data  
    '''
    for i in range(len(coin['results'])):
        timestamp_ms = int(coin['results'][i]['t'])
        timestamp_sec = timestamp_ms / 1000
        readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)

        cur.execute(
            "INSERT OR IGNORE INTO Nvidia (id, timestamp, date, open, close, high, low) VALUES (?,?,?,?,?,?,?)", (counter,
                                                                                                            coin['results'][i]['t'],
                                                                                                            str(readable_date),
                                                                                                            coin['results'][i]['o'],
                                                                                                            coin['results'][i]['c'],
                                                                                                            coin['results'][i]['h'],
                                                                                                            coin['results'][i]['l'])                                                                                                        
        )
        counter +=1

    conn.commit()

    return counter

def add_stockdata_to_table(coin, cur, conn, counter):
    '''
    Data = List of dictionaries with market data  
    '''
    for i in coin.values():
        timestamp_ms = int(i['timestamp'])
        timestamp_sec = timestamp_ms / 1000
        readable_date = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        cur.execute(
            "INSERT OR IGNORE INTO Nasdaq (id, timestamp, date, open, close, high, low) VALUES (?,?,?,?,?,?,?)", (counter,
                                                                                                      i['timestamp'],
                                                                                                      str(readable_date),
                                                                                                      i['open'],
                                                                                                      i['close'],
                                                                                                      i['high'],
                                                                                                      i['low'])                                                                                                        
        )
        counter += 1
        
    conn.commit()

    return counter

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
        else:
            print("Invalid input. Please try again.")
            continue
    
def get_dates(api):
    print("Let's collect some data!")
    if api == "NASDAQ":
        try:
            print("You have selected to pull data from the NASDAQ API.")
            s_year = input("Please enter the year you would like to begin pulling data from (YYYY): ")
            s_month = input("Please enter the month you would like to begin pulling data from (MM): ")
            s_day = input("Please enter the day you would like to begin pulling data from (DD): ")
            e_year = input("Please enter the year you would like to end pulling data from (YYYY): ")
            e_month = input("Please enter the month you would like to end pulling data from (MM): ")
            eday = input("Please enter the day you would like to end pulling data from (DD): ")
            star_date = (int(s_year), int(s_month), int(s_day))
            end_date = (int(e_year), int(e_month), int(eday))
            
            return star_date, end_date
        
        except:
            print("Invalid input. Please try again.")
            return None



def main():

    
    ######################################################################### SETUP
    # Set up database
    database_name = "NEW3_final_project.db"
    cur, conn = set_up_database(database_name) 

    # Creates tables POSTS and ENGAGEMENT
    # set_up_posts_tables(cur, conn)
    set_up_market_coin_table(cur, conn)

    api = menu_apis()
    year, month, day = get_start_date(api)

    # Set up start date
    offset = timezone(timedelta(hours=2))
    start_date = datetime(year, month, day, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=25)

    # Set Start and End Dates
    offset = timezone(timedelta(hours=2))
    dt_start = datetime(2025, 3, 30, tzinfo=offset)
    dt_end = datetime(2025, 4, 16, tzinfo=offset)

    ######################################################################### Calling API Calls
    # Calculate the range between two datetime objects
    # date_range = (dt_end - dt_start).days
    # print(f"Range between dt_start and dt_end in days: {date_range}")

    # p = Polygon()
    # p.get_stonks(str(datetime(2024, 11, 1, tzinfo=offset))[0:10], str(datetime(2024, 11, 26, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2024, 11, 1, tzinfo=offset))[0:10], str(datetime(2024, 11, 26, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2024, 11, 1, tzinfo=offset), datetime(2024, 11, 26, tzinfo=offset))
    # time.sleep(10)

    # p.get_stonks(str(datetime(2024, 11, 26, tzinfo=offset))[0:10], str(datetime(2024, 12, 21, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2024, 11, 26, tzinfo=offset))[0:10], str(datetime(2024, 12, 21, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2024, 11, 26, tzinfo=offset), datetime(2024, 12, 21, tzinfo=offset))
    # time.sleep(10)

    # p.get_stonks(str(datetime(2024, 12, 21, tzinfo=offset))[0:10], str(datetime(2025, 1, 15, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2024, 12, 21, tzinfo=offset))[0:10], str(datetime(2025, 1, 15, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2024, 12, 21, tzinfo=offset), datetime(2025, 1, 15, tzinfo=offset))
    # time.sleep(10)

    # p.get_stonks(str(datetime(2025, 1, 15, tzinfo=offset))[0:10], str(datetime(2025, 2, 9, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2025, 1, 15, tzinfo=offset))[0:10], str(datetime(2025, 2, 9, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2025, 1, 15, tzinfo=offset), datetime(2025, 2, 9, tzinfo=offset))
    # time.sleep(10)

    # p.get_stonks(str(datetime(2025, 2, 9, tzinfo=offset))[0:10], str(datetime(2025, 3, 6, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2025, 2, 9, tzinfo=offset))[0:10], str(datetime(2025, 3, 6, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2025, 2, 9, tzinfo=offset), datetime(2025, 3, 6, tzinfo=offset))
    # time.sleep(10)

    # p.get_stonks(str(datetime(2025, 3, 6, tzinfo=offset))[0:10], str(datetime(2025, 3, 31, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2025, 3, 6, tzinfo=offset))[0:10], str(datetime(2025, 3, 31, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2025, 3, 6, tzinfo=offset), datetime(2025, 3, 31, tzinfo=offset))
    # time.sleep(10)

    # p.get_stonks(str(datetime(2025, 3, 31, tzinfo=offset))[0:10], str(datetime(2025, 4, 17, tzinfo=offset))[0:10])
    # get_stonks_finage("NVDA", str(datetime(2025, 3, 31, tzinfo=offset))[0:10], str(datetime(2025, 4, 17, tzinfo=offset))[0:10])
    # coin_candles("BTC-USD", datetime(2025, 3, 31, tzinfo=offset), datetime(2025, 4, 17, tzinfo=offset))
    # time.sleep(10)
    


    # for i in range(date_range):
    #     dt_new_start = dt_start + timedelta(days=i)
    #     dt_new_end = dt_new_start + timedelta(days=3)
    #     # # Call Polygon API to retrieve NASDAQ Indicie data per the dates given, return JSON
    #     p = polygon()
    #     p.get_stonks(dt_new_start, dt_new_end)

    #     # # Call Coinbase API to retrieve Bitcoin data per the dates given, return JSON
    #     coin_candles("BTC-USD", dt_new_start, dt_new_end)
    #     print("waiting for 12s before next...")
    #     time.sleep(12)
    #     print("moving on")
    #     print("----------------------------------")

    # Call Truthbrush to scrape posts (May get rate limited)
    # print("Done")
    # truth_pull_posts(Api(), "realdonaldtrump", dt_start, dt_end)

    
    
    
    ######################################################################### Adding data to tables
   
    
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

    ncounter = 1
    scounter = 1
    ccounter = 1
    
    for i in range(7):

        # Pulls data from json and make it into list of dictionaries
        polygon = get_json_content(polygon_list[i])
        finage = get_json_content(finage_list[i])
        cripto = get_json_content(cripto_list[i])

        nnum = add_nvdadata_to_table(finage, cur, conn, ncounter)
        snum = add_stockdata_to_table(polygon, cur, conn, scounter)
        cnum = add_criptodata_to_table(cripto, cur, conn, ccounter)

        ncounter = nnum
        ccounter = cnum
        scounter = snum
    


    # pass




if __name__ == '__main__':
    main()
    