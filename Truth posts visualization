import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

# Function to fetch data from the database
def fetch_post_data(connection):
    query = "SELECT date, count FROM PostPerDay"
    df = pd.read_sql_query(query, connection)
    return df

# Function to visualize the posts per day
def visualize_posts_per_day(df):
    df['date'] = pd.to_datetime(df['date'])
    
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['count'], marker='o', linestyle='-', color='c')
    plt.title('Posts Per Day')
    plt.xlabel('Date')
    plt.ylabel('Count of Posts')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

path = os.path.dirname(os.path.abspath(__file__))
    
database_file_path = path

connection = sqlite3.connect(database_file_path + "/" + "truthbrush_postsCOUNT.db")

posts_data = fetch_post_data(connection)

visualize_posts_per_day(posts_data)

connection.close()