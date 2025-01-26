from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def connect_to_hive():
    try:
        conn = hive.Connection(host='localhost', port=10000, database='default')
        print("Connected to Hive")
        return conn
    except Exception as e:
        print(f"Error connecting to Hive: {e}")
        return None

def get_user_input():
    try:
        year = input("Enter the year (e.g., 2025): ")
        month = input("Enter the month (e.g., 01 for January): ")
        day = input("Enter the day (e.g., 12): ")
        hour = input("Enter the hour (e.g., 07): ")
        minute = input("Enter the minute (e.g., 30): ")
        duration_minutes = int(input("Enter the duration in minutes (e.g., 5): "))

        start_time = datetime.strptime(f"{year}-{month}-{day} {hour}:{minute}:00", '%Y-%m-%d %H:%M:%S')
        end_time = start_time + timedelta(minutes=duration_minutes)

        print(f"Fetching data from {start_time} to {end_time}")
        return start_time, end_time
    except ValueError as e:
        print(f"Invalid input: {e}")
        return None, None

def fetch_high_power_data(conn, start_time, end_time, min_power):
    if start_time is None or end_time is None:
        print("Invalid time range. Exiting.")
        return pd.DataFrame()

    query = f"""
        SELECT 
            `timestamp`, 
            `global_active_power` 
        FROM electrical_read_2
        WHERE global_active_power > {min_power}
          AND `timestamp` >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
          AND `timestamp` < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        ORDER BY `timestamp`
    """

    print(f"Running query:\n{query}")
    try:
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from Hive.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def plot_high_power(df):
    if df.empty:
        print("No data available to plot.")
        return

    # Plot global active power over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['global_active_power'], marker='o', label='Global Active Power')
    plt.title('Global Active Power Over Time')
    plt.xlabel('Timestamp')
    plt.ylabel('Global Active Power (kW)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    conn = connect_to_hive()
    if conn:
        min_power = float(input("Enter the minimum Global Active Power (e.g., 8.0): "))
        start_time, end_time = get_user_input()

        if start_time and end_time:
            data = fetch_high_power_data(conn, start_time, end_time, min_power)
            plot_high_power(data)

        conn.close()
