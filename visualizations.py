# SI 206 Final Project
# Team name:
#   - DOJE
# Team members:
#   - Jack Bernard (jackber@umich.edu - UMID 43241772)
#   - Oleg Korobkov (olegko@umich.edu - UMID 58329022)
#   - Danielle Schulz (dfaria@umich.edu - UMID 63218489)

import os
import sqlite3
from datetime import datetime, timezone, timedelta
import json
from datetime import datetime
import time
import secrets
import pandas as pd
import matplotlib.pyplot as plt

def plot_individual_table(df, table_name):
    # Sort DataFrame by timestamp in ascending order
    df = df.sort_index()

    plt.figure(figsize=(14, 6))
    plt.plot(df.index, df['close'], label='Close', color='blue')
        
    plt.title(f'Close Price Over Time for {table_name}')
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'{table_name}_close_plot.png')
    plt.show()
    
def plot_comparative_tables(connection, tables):
    plt.figure(figsize=(14, 8))
    
    colors = ['b', 'g', 'r']  # Different colors for each table
    for i, table_name in enumerate(tables):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, connection)
        
        # Determine the timestamp unit based on the table name
        if table_name == 'Bitcoin':
            # Bitcoin timestamps are in seconds
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='s')
        else:
            # Other timestamps are in milliseconds
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms')
        
        # Set timestamp as the index
        df.set_index('timestamp', inplace=True)
        
        # Sort DataFrame by timestamp in ascending order
        df = df.sort_index()
        df['daily_percent_change'] = df['close'].pct_change() * 100
        plt.plot(df.index, df['daily_percent_change'], label=f'{table_name} - % Daily Change', c=colors[i])
    
    plt.title('Comparative % Daily Close Price Change Over Time')
    plt.xlabel('Time')
    plt.ylabel('% Change')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('comparative_daily_percent_change_plot.png')
    plt.show()

def plot_tables(connection, tables):
    for table_name in tables:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, connection)
        
        # Determine the timestamp unit based on the table name
        if table_name == 'Bitcoin':
            # Bitcoin timestamps are in seconds
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='s')
        else:
            # Other timestamps are in milliseconds
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms')
        
        # Set timestamp as the index
        df.set_index('timestamp', inplace=True)
        
        plot_individual_table(df, table_name)
    
    # After plotting individual tables, plot them together for comparative analysis
    plot_comparative_tables(connection, tables)

    

def main():
    # Path to your .db file
    path = os.path.dirname(os.path.abspath(__file__))
    
    database_file_path = path

    connection = sqlite3.connect(database_file_path)

    # List of tables to visualize
    tables = ['Bitcoin', 'Nasdaq', 'Nvidia']

    # Plot each table's data separately and then a comparative view
    plot_tables(connection, tables)

    # Close the database connection
    connection.close()

if __name__ == '__main__':
    main()