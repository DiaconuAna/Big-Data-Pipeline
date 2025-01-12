from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


# Step 1: Connect to Hive
def connect_to_hive():
    try:
        conn = hive.Connection(host='localhost', port=10000, database='default')
        print("Connected to Hive")
        return conn
    except Exception as e:
        print(f"Error connecting to Hive: {e}")
        return None

# Step 2: Query Data Based on User Input
def fetch_data(conn):
    # Prompt the user for input
    year = input("Enter the year (e.g., 2025): ")
    month = input("Enter the month (e.g., 01 for January): ")
    day = input("Enter the day (e.g., 12): ")
    hour = input("Enter the hour (e.g., 07): ")
    minute = input("Enter the minute (e.g., 35): ")

    # Construct the start and end timestamps based on user input
    try:
        start_time = datetime.strptime(f"{year}-{month}-{day} {hour}:{minute}:00", '%Y-%m-%d %H:%M:%S')
        end_time = start_time + timedelta(minutes=5)  # 5-minute window

        # Construct the query
        query = f"""
            SELECT `timestamp`, `voltage`
            FROM electrical_read_1
            WHERE `timestamp` >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
              AND `timestamp` < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """

        print(f"Running query:\n{query}")
        # Execute the query and load into a DataFrame
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from Hive.")
        return df
    except ValueError as e:
        print(f"Invalid input: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

# Step 3: Visualize the Data
def plot_voltage(df):
    if df.empty:
        print("No data available to plot.")
        return

    # Plot voltage over time
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

# Main Function
if __name__ == '__main__':
    # Connect to Hive
    conn = connect_to_hive()
    if conn:
        # Fetch data based on user input
        data = fetch_data(conn)

        # Visualize the data
        plot_voltage(data)

        # Close the connection
        conn.close()
