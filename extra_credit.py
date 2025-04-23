import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

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

def set_up_posts_tables(cur, conn):
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Posts (post_id INTEGER PRIMARY KEY, timestamp TEXT, post_content TEXT, engagement INTEGER, day_key INTEGER, FOREIGN KEY(day_key) REFERENCES PostPerDay(day_key))"
    )
    
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Engagement (post_id INTEGER PRIMARY KEY, replies INTEGER, reblogs INTEGER, favourites INTEGER)"
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS PostPerDay (day_key INTEGER PRIMARY KEY, date TEXT UNIQUE, count INTEGER)"
    )
    
    conn.commit()


def add_posts_to_table(data, cur, conn):
    '''
    Data = List of dictionaries with post data  
    '''
    for post in data:
        post_id = post['id']
        timestamp = post['created_at']
        post_content = post['content']
        replies = post['replies_count']
        reblogs = post['reblogs_count']
        favourites = post['favourites_count']
        engagement = replies + reblogs + favourites

        # Extract date from timestamp
        date_str = timestamp.split("T")[0]

        # Check if date exists in PostPerDay
        cur.execute("SELECT day_key, count FROM PostPerDay WHERE date = ?", (date_str,))
        result = cur.fetchone()

        if result:
            day_key, count = result
            cur.execute("UPDATE PostPerDay SET count = ? WHERE day_key = ?", (count + 1, day_key))
        else:
            # Insert new date with initial count = 1
            cur.execute("INSERT INTO PostPerDay (date, count) VALUES (?, ?)", (date_str, 1))
            day_key = cur.lastrowid  # get auto-incremented key

        # Insert into Engagement
        cur.execute(
            "INSERT OR IGNORE INTO Engagement (post_id, replies, reblogs, favourites) VALUES (?, ?, ?, ?)",
            (post_id, replies, reblogs, favourites)
        )

        # Insert into Posts with day_key
        cur.execute(
            "INSERT OR IGNORE INTO Posts (post_id, timestamp, post_content, engagement, day_key) VALUES (?, ?, ?, ?, ?)",
            (post_id, timestamp, post_content, engagement, day_key)
        )

    conn.commit()

def main():
    cur, conn = set_up_database("truthbrush_postsCOUNT.db")
    data = get_json_content("REAL_statuses.json")
    set_up_posts_tables(cur, conn)
    add_posts_to_table(data, cur, conn)


if __name__ == '__main__':
    main()
    