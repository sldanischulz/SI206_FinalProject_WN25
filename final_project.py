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
import json
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

# Truth Social API Request
def truth_user_lookup(api, handle):
    """Pulls a user's basic information from Truthsocial and stores it in a .json"""

    user = api.lookup(handle)
    with open(f"{handle}_info.json", "w") as json_file:
        json.dump(user, json_file, indent=4)
    return user

def truth_pull_posts(api, handle, start_date):
    """Pulls a user's posts after a defined date from Truthsocial and stores it in a .json"""

    partial_pull = list(api.pull_statuses(username=handle, replies=False, created_after=start_date, verbose=True))
    partial_pull = partial_pull[0:25]
    with open(f"{start_date}_{handle}_statuses.json", "w") as json_file:
        json.dump(partial_pull, json_file, indent=4)
    return partial_pull

# Historical Stock Market Data API Request
class polygon():
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
            
        with open(f"stocks_{str(date_start)[0:10]}_to_{str(date_end)[0:10]}.json", "w") as json_file:
            json.dump(new_aggs_dict, json_file, indent=4)
        return

offset = timezone(timedelta(hours=2))
dt_start = datetime(2025, 3, 31, tzinfo=offset)
dt_end = datetime(2025, 4, 4, tzinfo=offset)
# truth_user_lookup(Api(), "realdonaldtrump")
# truth_pull_posts(Api(), "realdonaldtrump", dt_start)
# p = polygon()
# p.get_stonks(dt_start, dt_end)

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
    with open(f"crypto_candles_{str(start_date)[0:10]}.json", "w") as json_file:
        json.dump(json.loads(decoded_data), json_file, indent=4)

# candles = coin_candles("BTC-USD", dt_start, dt_end)
# print(candles)

def get_currency_rates():
    # NOT WORKING BECAUSE WE WOULD NEED TO PAY FOR THIS ONE
    conn = http.client.HTTPSConnection("api.currencyfreaks.com")
    payload = ''
    headers = {}
    conn.request("GET", "/v2.0/rates/historical?date=2022-03-20&base=usd&symbols=gbp,eur,pkr,cad&apikey=dc64885d5e1047888acca2ff0451b30f", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return(data.decode("utf-8"))

# curr = get_currency_rates()
# print(curr)

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
        "CREATE TABLE IF NOT EXISTS Posts (post_id INTEGER PRIMARY KEY, timestamp TEXT, post_content TEXT, engagement INTEGER)"
    )
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Engagement (post_id INTEGER PRIMARY KEY, replies INTEGER, reblogs INTEGER, favourites INTEGER)"
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

def set_up_market_table(data, cur, conn):
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Market (id INTEGER PRIMARY KEY, timestamp TEXT, open INTEGER, close INTEGER, high INTEGER, low INTEGER)"
    )

    conn.commit()

def add_marketdata_to_table(data, cur, conn):
    '''
    Data = List of dictionaries with market data  
    '''

    for i in range(len(data)):

        cur.execute(
            "INSERT OR IGNORE INTO Market (id, timestamp, open, close, high, low) VALUES (?,?,?,?,?,?)", (i,
                                                                                                          data[i]['created_at'],
                                                                                                          data[i]['open'],
                                                                                                          data[i]['close'],
                                                                                                          data[i]['high'],
                                                                                                          data[i]['low'])                                                                                                        
        )

    conn.commit()



def main():
    ######################################################################### SETUP
    # Set up database
    cur, conn = set_up_database("final_project.db") 

    # Creates tables POSTS and ENGAGEMENT
    set_up_posts_tables(cur, conn)

    ######################################################################### Adding data to tables
    # Pulls data from json and make it into list of dictionaries
    posts = get_json_content("realdonaldtrump_statuses.json")
    #print(data) # Test to visualize the data

    # Adds info from dictionary into the database
    add_posts_to_table(posts, cur, conn)
    #pass

if __name__ == '__main__':
    main()
    