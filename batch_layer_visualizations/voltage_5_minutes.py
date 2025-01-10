from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


# Step 2: Connect to Hive
def connect_to_hive():
    try:
        conn = hive.Connection(host='localhost', port=10000, database='default')
        print("Connected to Hive")
        return conn
    except Exception as e:
        print(f"Error connecting to Hive: {e}")
        return None

# Step 3: Query Data for the Last 2 Hours
def fetch_data(conn):
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=5)  # Last 5 minutes
    start_hour_time = end_time - timedelta(hours=3)  # 3 hours earlier

    # Extract partition values for start and end times
    start_partition = {
        'year': start_hour_time.strftime('%Y'),
        'month': start_hour_time.strftime('%m'),
        'day': start_hour_time.strftime('%d'),
        'hour': start_hour_time.strftime('%H'),
        'minute': start_time.strftime('%M'),  # Current minute from start time
    }
    end_partition = {
        'year': end_time.strftime('%Y'),
        'month': end_time.strftime('%m'),
        'day': end_time.strftime('%d'),
        'hour': start_hour_time.strftime('%H'),
        'minute': end_time.strftime('%M'),
    }

    # Construct the query using partition filters
    query = f"""
        SELECT `timestamp`, `voltage`
        FROM electrical_read_1
        WHERE
          (
            (year = '{start_partition['year']}' AND month = '{start_partition['month']}' AND
             day = '{start_partition['day']}' AND hour = '{start_partition['hour']}' AND
             minute >= '{start_partition['minute']}')
          )
          OR
          (
            (year = '{end_partition['year']}' AND month = '{end_partition['month']}' AND
             day = '{end_partition['day']}' AND hour = '{end_partition['hour']}' AND
             minute <= '{end_partition['minute']}')
          )
    """

    print(query)
    try:
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from Hive.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

# Step 4: Visualize the Data
def plot_voltage(df):
    if df.empty:
        print("No data available to plot.")
        return

    # Plot voltage over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['voltage'], marker='o', label='Voltage')
    plt.title('Voltage Over the Last 5 Minutes')
    plt.xlabel('Timestamp')
    plt.ylabel('Voltage (V)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

# Main Function
if __name__ == '__main__':
    # Step 1: Connect to Hive
    conn = connect_to_hive()
    if conn:
        # Step 2: Fetch Data
        data = fetch_data(conn)

        # Step 3: Visualize Data
        plot_voltage(data)

        # Close the connection
        conn.close()
