from pyhive import hive
from tensorflow.keras.models import load_model
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import time
import numpy as np
import random


# Hive connection configuration
HIVE_HOST = 'host.docker.internal'
HIVE_PORT = 10000
HIVE_DATABASE = 'default'

# Cassandra connection configuration
CASSANDRA_HOST = '172.22.0.6'
CASSANDRA_USERNAME = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'
CASSANDRA_KEYSPACE = 'electrical'

# Model configuration
MODEL_PATH = "lstm_config.h5"


def connect_to_hive():
    """Establishes a connection to the Hive server."""
    return hive.Connection(host=HIVE_HOST, port=HIVE_PORT, database=HIVE_DATABASE)


def connect_to_cassandra():
    """Establishes a connection to the Cassandra cluster."""
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)
    cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session


def write_prediction_to_cassandra(session, sensor_id, predictions, timestamp):
    """
    Inserts prediction data into the Cassandra database.

    Args:
        session: Cassandra session object.
        sensor_id: The sensor ID for the prediction.
        predictions: The predicted values as a list.
        timestamp: The timestamp of the prediction.
    """
    prediction_time = datetime.fromtimestamp(timestamp)
    query = """
    INSERT INTO predictions (sensor_id, prediction_time, prediction1, prediction2, prediction3, prediction4, prediction5, prediction6)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (sensor_id, prediction_time, *predictions))


def get_last_rows(conn):
    """
    Fetches the last 30 rows from the Hive table.

    Args:
        conn: Hive connection object.

    Returns:
        A reversed NumPy array of the last 30 rows.
    """
    # Repair table to ensure latest metadata is loaded
    cursor = conn.cursor()
    cursor.execute("MSCK REPAIR TABLE electrical_read_2")
    cursor.close()

    # Fetch the last 30 rows
    query = "SELECT global_active_power, `timestamp` FROM electrical_read_2 ORDER BY `timestamp` DESC LIMIT 30"
    cursor = conn.cursor()
    cursor.execute(query)

    data = np.array(cursor.fetchall())
    cursor.close()

    return data[::-1]  # Reverse the order of rows to maintain their natural order



def prepare_data_for_prediction(data):
    """
    Prepares input data for the LSTM model.

    Args:
        data: NumPy array of input data.

    Returns:
        A reshaped NumPy array suitable for the LSTM model.
    """
    numerical_data = np.array(data[:, 0], dtype=np.float32)
    return numerical_data.reshape(1, len(numerical_data), 1)


def main():
    """Main function to handle the data flow and predictions."""
    # Establish connections
    conn = connect_to_hive()
    session = connect_to_cassandra()

    # Load the trained model
    model = load_model(MODEL_PATH)
    print(f"Model loaded from {MODEL_PATH}")

    # Wait for dependencies to be ready
    time.sleep(60)

    # Prediction loop
    while True:
        try:
            # Fetch and prepare data
            rows = get_last_rows(conn)
            X = prepare_data_for_prediction(rows)
            print("Prepared input data:", X)

            # Predict and randomize outputs
            raw_output = model.predict(X)
            predicted_output = relu(raw_output)
            print(f"Predicted output: {predicted_output}")

            # Write predictions to Cassandra
            write_prediction_to_cassandra(session, sensor_id=1, predictions=predicted_output, timestamp=time.time())

        except Exception as e:
            print(f"An error occurred: {e}")

        # Wait before next iteration
        time.sleep(60)


if __name__ == "__main__":
    main()
