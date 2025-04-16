# SI 206 Final Project
# Team name:
#   - TBD
# Team members:
#   - Jack Bernard (jackber@umich.edu - UMID 43241772)
#   - Oleg Korobkov (olegko@umich.edu - UMID 58329022)
#   - Danielle Schulz (dfaria@umich.edu - UMID 63218489)

from bs4 import BeautifulSoup
import re
import os
import requests
import json
import sqlite3
from truthbrush.api import Api
from datetime import datetime, timezone
import json
from polygon import RESTClient
from datetime import datetime

'''
Abstract:
API's:
    - Truthbrush
    - Market one
    - Trendy word one

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
    with open(f"{handle}_statuses.json", "w") as json_file:
        json.dump(partial_pull, json_file, indent=4)
    return partial_pull

# Historical Stock Market Data API Request
class polygon():
    """Initializes the Polygon API client with the API key."""
    def __init__(self):
        # Read the API key from a file
        # Change the path to the location of your apikey.txt file
        with open(r'c:\Users\jackb\Documents\School\YEAR2\Winter\SI206\Project\Final Proj\apikey.txt', 'r') as file:
            self.key = file.read().strip()
        pass
    def get_stonks(self, date_start, date_end):
        """Pulls historical stock market data from Polygon API and stores it in a .json file."""
        
        client = RESTClient(self.key)

        aggs = []
        for a in client.list_aggs(
            "TSLA",
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
            

        # aggs_dict = [
        #     {
        #     "open": a.open,
        #     "high": a.high,
        #     "low": a.low,
        #     "close": a.close,
        #     "volume": a.volume,
        #     "vwap": a.vwap,
        #     "timestamp": a.timestamp,
        #     "transactions": a.transactions,
        #     "otc": a.otc,
        #     }
        #     for a in aggs
        # ]
        # print(type(aggs_dict[0]))
        with open(f"stocks_{date_start}_to_{date_end}.json", "w") as json_file:
            json.dump(new_aggs_dict, json_file, indent=4)
        return

# truth_user_lookup(Api(), "realdonaldtrump")
# truth_pull_posts(Api(), "realdonaldtrump", "2025-03-31")
p = polygon()
p.get_stonks("2025-03-31", "2025-04-04")

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
            "INSERT OR IGNORE INTO Engagement (post_id, replies_count, reblogs_count, favourites_count) VALUES (?,?,?,?)",
            (data[i]['post_id'], rep, reb, fav)
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

def add_posts_to_table(data, cur, conn):
    '''
    Data = List of dictionaries with market data  
    '''

    for i in range(len(data)):

        cur.execute(
            "INSERT OR IGNORE INTO Market (id, timestamp, open, close, high, low) VALUES (?,?,?,?,?,?)", (i,
                                                                                                          data[i]['timestamp'],
                                                                                                          data[i]['open'],
                                                                                                          data[i]['close'],
                                                                                                          data[i]['high'],
                                                                                                          data[i]['low'])                                                                                                        
        )

    conn.commit()



def main():
    pass

if __name__ == '__main__':
    main()
    