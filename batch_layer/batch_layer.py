from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, from_unixtime, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "electrical_read"
HDFS_OUTPUT_DIR = "hdfs://namenode:8020/electrical_data"

# Define schema for Kafka data
schema = StructType([
    StructField("time", LongType(), True),
    StructField("global_active_power", DoubleType(), True),
    StructField("global_reactive_power", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("global_intensity", DoubleType(), True),
    StructField("sub_metering_1", DoubleType(), True),
    StructField("sub_metering_2", DoubleType(), True),
    StructField("sub_metering_3", DoubleType(), True),
])

#
# def write_to_hdfs(batch_df, batch_id):
#     # Add structured paths
#     batch_df = batch_df.withColumn("year", date_format(col("timestamp"), "yyyy")) \
#         .withColumn("month", date_format(col("timestamp"), "MM")) \
#         .withColumn("day", date_format(col("timestamp"), "dd")) \
#         .withColumn("hour", date_format(col("timestamp"), "HH"))
#
#     # Write each batch to HDFS in the desired folder structure
#     batch_df.write.partitionBy("year", "month", "day", "hour") \
#         .mode("append") \
#         .json(HDFS_OUTPUT_DIR)


# def write_to_hdfs(batch_df, batch_id):
#     # Add additional columns to group by
#     batch_df = batch_df.withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))
#
#     batch_df = batch_df.withColumn("year", date_format("timestamp", "yyyy")) \
#         .withColumn("month", date_format("timestamp", "MM")) \
#         .withColumn("day", date_format("timestamp", "dd")) \
#         .withColumn("hour", date_format("timestamp", "HH"))
#
#     # Convert to Pandas for easier grouping and writing, as you wanted a similar approach to pandas
#     batch_df_pandas = batch_df.toPandas()
#
#     for hour, group in batch_df_pandas.groupby(batch_df_pandas['timestamp'].dt.floor('h')):
#         # Create a structured folder path based on the hour
#         output_dir_path = f'{HDFS_OUTPUT_DIR}/years={hour.strftime("%Y")}/months={hour.strftime("%m")}/days={hour.strftime("%d")}/hours={hour.strftime("%H")}'
#
#         # Create directories if they don't exist
#         os.makedirs(output_dir_path, exist_ok=True)
#
#         # Define the output file path
#         output_file_path = os.path.join(output_dir_path, 'file.json')
#
#         # Drop the 'timestamp' column if necessary
#         group = group.drop(columns=['timestamp'])
#
#         # Write the group to a JSON file
#         group.to_json(output_file_path, orient='records', lines=True)
#
#     print(f"Batch {batch_id} written successfully to HDFS with the structured folder paths.")  # Print for confirmation

def write_to_hdfs(batch_df, batch_id):
    # Add additional columns to group by
    batch_df = batch_df.withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))

    # Add year, month, day, and hour columns
    batch_df = batch_df.withColumn("year", date_format("timestamp", "yyyy")) \
        .withColumn("month", date_format("timestamp", "MM")) \
        .withColumn("day", date_format("timestamp", "dd")) \
        .withColumn("hour", date_format("timestamp", "HH")) \
        .withColumn("minute", date_format("timestamp", "mm"))

    # Group by year, month, day, hour, and minute
    grouped_df = batch_df.groupBy("year", "month", "day", "hour", "minute").agg(
        collect_list("timestamp").alias("values"))

    # Iterate over the grouped DataFrame and save each group to HDFS
    for row in grouped_df.collect():
        year = row['year']
        month = row['month']
        day = row['day']
        hour = row['hour']
        minute = row['minute']
        values = row['values']

        # Create a structured folder path based on the year, month, day, and hour
        output_dir_path = f'{HDFS_OUTPUT_DIR}/years={year}/months={month}/days={day}/hours={hour}/minutes={minute}'

        # Create directories if they don't exist
        os.makedirs(output_dir_path, exist_ok=True)

        # Define the output file path
        output_file_path = os.path.join(output_dir_path, 'file.json')

        # Write the values to a JSON file using Spark's built-in JSON writer
        # Convert the values back into a DataFrame for writing to HDFS
        group_df = spark.createDataFrame([(value,) for value in values], ['timestamp'])  # Adjust column name as needed
        group_df.write.mode('overwrite').json(output_file_path)

    print(f"Batch {batch_id} written successfully to HDFS with the structured folder paths.")  # Print for confirmation


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BatchLayer") \
        .getOrCreate()

    # Read data from Kafka
    raw_data = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Parse Kafka data
    json_data = raw_data.select(from_json(col("value").cast("string"), schema).alias("data"))
    parsed_data = json_data.select(
        col("data.time").alias("timestamp"),
        col("data.global_active_power"),
        col("data.global_reactive_power"),
        col("data.voltage"),
        col("data.global_intensity"),
        col("data.sub_metering_1"),
        col("data.sub_metering_2"),
        col("data.sub_metering_3"),
    )

    print("Parsed data: ", json_data)
    # Write to HDFS
    query = parsed_data.writeStream \
        .foreachBatch(write_to_hdfs) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
