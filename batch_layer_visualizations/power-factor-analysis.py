from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Step 1: Connect to Hive
def connect_to_hive():
    try:
        conn = hive.Connection(host='localhost', port=10000, database='default')
        print("Connected to Hive")
        return conn
    except Exception as e:
        print(f"Error connecting to Hive: {e}")
        return None

# Step 2: Get User Input for Query Parameters
def get_user_input():
    try:
        # Prompt user for start date and time
        start_year = input("Enter the start year (e.g., 2025): ")
        start_month = input("Enter the start month (e.g., 01): ")
        start_day = input("Enter the start day (e.g., 10): ")
        start_hour = input("Enter the start hour (e.g., 00): ")

        # Prompt user for end date and time
        end_year = input("Enter the end year (e.g., 2025): ")
        end_month = input("Enter the end month (e.g., 01): ")
        end_day = input("Enter the end day (e.g., 10): ")
        end_hour = input("Enter the end hour (e.g., 23): ")

        # Validate and construct the timestamps
        start_time = datetime.strptime(f"{start_year}-{start_month}-{start_day} {start_hour}:00:00", '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(f"{end_year}-{end_month}-{end_day} {end_hour}:00:00", '%Y-%m-%d %H:%M:%S')

        if start_time >= end_time:
            print("Start time must be earlier than end time.")
            return None, None

        print(f"Fetching data from {start_time} to {end_time}")
        return start_time, end_time
    except ValueError as e:
        print(f"Invalid input: {e}")
        return None, None

# Step 3: Query Data for Power Factor Calculation
def fetch_power_factor_data(conn, start_time, end_time):
    if not conn or not start_time or not end_time:
        print("Invalid connection or time range.")
        return pd.DataFrame()

    # Construct the query
    query = f"""
        SELECT
            `timestamp`,
            `global_active_power`,
            `global_reactive_power`,
            (global_active_power / SQRT(POW(global_active_power, 2) + POW(global_reactive_power, 2))) AS power_factor
        FROM electrical_read_2
        WHERE `timestamp` >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
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

# Step 4: Visualize the Data
def plot_power_factor(df):
    if df.empty:
        print("No data available to plot.")
        return

    # Plot power factor over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['power_factor'], marker='o', label='Power Factor')
    plt.title('Power Factor Over Time')
    plt.xlabel('Timestamp')
    plt.ylabel('Power Factor')
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
        # Step 2: Get User Input
        start_time, end_time = get_user_input()

        # Step 3: Fetch Data
        if start_time and end_time:
            data = fetch_power_factor_data(conn, start_time, end_time)

            # Step 4: Visualize Data
            plot_power_factor(data)

        # Close the connection
        conn.close()
