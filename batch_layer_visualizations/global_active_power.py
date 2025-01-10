from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Connect to Hive
def connect_to_hive():
    try:
        conn = hive.Connection(host='localhost', port=10000, database='default')
        print("Connected to Hive")
        return conn
    except Exception as e:
        print(f"Error connecting to Hive: {e}")
        return None

# Step 2: Query Data for Global Active Power > 5.0
def fetch_high_power_data(conn):
    query = """
        SELECT 
            `timestamp`, 
            `global_active_power` 
        FROM electrical_read_1
        WHERE global_active_power > 5.0
        ORDER BY `timestamp`
    """

    print(query)
    try:
        df = pd.read_sql(query, conn)
        print(f"Fetched {len(df)} rows from Hive.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

# Step 3: Visualize the Data
def plot_high_power(df):
    if df.empty:
        print("No data available to plot.")
        return

    # Plot global active power over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['global_active_power'], marker='o', label='Global Active Power')
    plt.title('Global Active Power > 5.0 Over Time')
    plt.xlabel('Timestamp')
    plt.ylabel('Global Active Power (kW)')
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
        data = fetch_high_power_data(conn)

        # Step 3: Visualize Data
        plot_high_power(data)

        # Close the connection
        conn.close()
