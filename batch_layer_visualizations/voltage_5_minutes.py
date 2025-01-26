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

def fetch_data(conn):
    year = input("Enter the year (e.g., 2025): ")
    month = input("Enter the month (e.g., 01 for January): ")
    day = input("Enter the day (e.g., 12): ")
    hour = input("Enter the hour (e.g., 07): ")
    minute = input("Enter the minute (e.g., 35): ")

    try:
        start_time = datetime.strptime(f"{year}-{month}-{day} {hour}:{minute}:00", '%Y-%m-%d %H:%M:%S')
        end_time = start_time + timedelta(minutes=5)  # 5-minute window

        query = f"""
            SELECT `timestamp`, `voltage`
            FROM electrical_read_2
            WHERE `timestamp` >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
              AND `timestamp` < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """

        print(f"Running query:\n{query}")
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from Hive.")
        return df
    except ValueError as e:
        print(f"Invalid input: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def plot_voltage(df):
    if df.empty:
        print("No data available to plot.")
        return

    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['voltage'], marker='o', label='Voltage')
    plt.title('Voltage Over Time')
    plt.xlabel('Timestamp')
    plt.ylabel('Voltage (V)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    conn = connect_to_hive()
    if conn:
        data = fetch_data(conn)
        plot_voltage(data)

        conn.close()
