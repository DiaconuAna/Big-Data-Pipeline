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

# Step 2: Query Data for Power Factor Calculation
def fetch_power_factor_data(conn):
    query = """
        SELECT
            `timestamp`,
            `global_active_power`,
            `global_reactive_power`,
            (global_active_power / SQRT(POW(global_active_power, 2) + POW(global_reactive_power, 2))) AS power_factor
        FROM electrical_read_1
        WHERE year = '2025' AND month = '01' AND day = '10'
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
def plot_power_factor(df):
    if df.empty:
        print("No data available to plot.")
        return

    # Plot power factor over time
    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['power_factor'], marker='o', label='Power Factor')
    plt.title('Power Factor Over Time (2025-01-10)')
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
        # Step 2: Fetch Data
        data = fetch_power_factor_data(conn)

        # Step 3: Visualize Data
        plot_power_factor(data)

        # Close the connection
        conn.close()
