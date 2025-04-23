import json
import os
import sqlite3

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

def main():
    cur, conn = set_up_database("truthbrush_posts.db")
    data = get_json_content("REAL_statuses.json")
    set_up_posts_tables(cur, conn)
    add_posts_to_table(data, cur, conn)
    

if __name__ == '__main__':
    main()
    