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

# This script visualizes data from a SQLite database containing Bitcoin, Nasdaq, and Nvidia stock prices.

# This function generates individual plots for each selected table
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

# This function generates a comparative plot for the daily percent change of close prices across multiple tables.
# It retrieves the data from each table, calculates the daily percent change, and plots them on the same graph.    
def plot_comparative_tables(connection, tables):
    plt.figure(figsize=(14, 8))
    
    colors = ['b', 'g', 'r']
    for i, table_name in enumerate(tables):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, connection)
        
        if table_name == 'Bitcoin':
            # Bitcoin timestamps are in seconds
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='s')
        else:
            # Other timestamps are in milliseconds
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms')
        
        # Set timestamp as the index
        df.set_index('timestamp', inplace=True)
        
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

# This function retrieves data from the specified tables in the SQLite database, processes it, and generates plots.
# It retrieves the data from each table, processes the timestamps, and generates individual plots for each table.
# It also generates a comparative plot for the daily percent change of close prices across all tables.
# It then saves the plots as PNG files and displays them.
def plot_tables(connection, tables):
    for table_name in tables:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, connection)
        
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

# This function calculates the daily average close prices for Bitcoin, Nasdaq, and Nvidia.
# It retrieves the data from the database, joins the tables on the date, and calculates the average close price for each date.
def calculate_daily_averages(connection):
    # The date data from the Nvidia table is used for joining to avoid ambiguity
    query = '''
    SELECT Nvidia.date AS date, 
           AVG(COALESCE(Bitcoin.close, 0) + COALESCE(Nasdaq.close, 0) + COALESCE(Nvidia.close, 0)) AS avg_close
    FROM 
        Bitcoin
    LEFT JOIN Nasdaq ON Bitcoin.id = Nasdaq.id
    LEFT JOIN Nvidia ON Bitcoin.id = Nvidia.id
    GROUP BY Nvidia.date
    '''
    
    return pd.read_sql_query(query, connection)


# This function writes the daily average close prices to a text file.
# It includes a header explaining the contents of the file and formats the data for readability.
def write_averages_to_file(df, filename):
    with open(filename, 'w') as f:
        f.write("This file contains the average of the 'close' values for Bitcoin, Nasdaq, and Nvidia per day. "
                "The averages are calculated by summing the closing values reported on the same date and dividing "
                "by the total number of tables providing data for that date. When a table lacks a 'close' value "
                "for a specific date, it adds 0 to the total.")
        f.write("Daily Average Close Prices\n")
        f.write("==========================\n")
        f.write("Date, Average Close\n")
        for index, row in df.iterrows():
            f.write(f"{row['date']}, {row['avg_close']:.2f}\n")
        f.write("\n")

# This function visualizes the daily average close prices using a line plot.
# It ensures the date column is in datetime format, handles missing values, and plots the data.
def visualize_averages(df):
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Drop rows with missing dates if any
    df = df.dropna(subset=['date'])
    
    plt.figure(figsize=(14, 6))
    plt.plot(df['date'], df['avg_close'], marker='o', linestyle='-')
    plt.title('Daily Average Close Prices')
    plt.xlabel('Date')
    plt.ylabel('Average Close Price')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()



def main():
    # Path to a .db file
    path = os.path.dirname(os.path.abspath(__file__))
    
    database_file_path = path

    connection = sqlite3.connect(database_file_path + "/" + "final_project.db")

    # List of tables to visualize
    tables = ['Bitcoin', 'Nasdaq', 'Nvidia']

    # Calculate daily averages
    daily_averages = calculate_daily_averages(connection)

    # Write output to a file
    write_averages_to_file(daily_averages, 'average_close_prices.txt')

    # Plot each table's data separately and then a comparative view
    plot_tables(connection, tables)

    visualize_averages(daily_averages)

    # Close the database connection
    connection.close()

if __name__ == '__main__':
    main()