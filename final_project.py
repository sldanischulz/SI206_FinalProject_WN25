# SI 206 Final Project
# Team name:
#   - TBD
# Team members:
#   - Jack Bernard 
#   - Oleg Korobkov
#   - Danielle Schulz (dfaria@umich.edu - UMID63218489)

from bs4 import BeautifulSoup
import re
import os
import requests
import json
import sqlite3

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
    