from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, from_unixtime, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "electrical_read"
HDFS_OUTPUT_DIR = "hdfs://namenode:8020/electrical_data/"

# Schema for Kafka data
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


def write_to_hdfs(batch_df, batch_id):
    # Add timestamp and partitioning columns
    batch_df = batch_df.withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))
    batch_df = batch_df.withColumn("year", date_format("timestamp", "yyyy")) \
        .withColumn("month", date_format("timestamp", "MM")) \
        .withColumn("day", date_format("timestamp", "dd")) \
        .withColumn("hour", date_format("timestamp", "HH")) \
        .withColumn("minute", date_format("timestamp", "mm"))

    # Write the data directly to HDFS in Hive-compatible partition structure
    output_dir_path = f"{HDFS_OUTPUT_DIR}"

    batch_df.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "hour", "minute") \
        .parquet(output_dir_path)

    print(f"Batch {batch_id} written successfully to HDFS in Parquet format for Hive integration.")
#
# def write_to_hdfs(batch_df, batch_id):
#     # Add timestamp and additional columns for grouping
#     batch_df = batch_df.withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))
#     batch_df = batch_df.withColumn("year", date_format("timestamp", "yyyy")) \
#         .withColumn("month", date_format("timestamp", "MM")) \
#         .withColumn("day", date_format("timestamp", "dd")) \
#         .withColumn("hour", date_format("timestamp", "HH")) \
#         .withColumn("minute", date_format("timestamp", "mm"))
#
#     # Group by year, month, day, hour, and minute
#     grouped_df = batch_df.groupBy("year", "month", "day", "hour", "minute").agg(
#         collect_list("timestamp").alias("timestamps"),
#         collect_list("global_active_power").alias("global_active_power"),
#         collect_list("global_reactive_power").alias("global_reactive_power"),
#         collect_list("voltage").alias("voltage"),
#         collect_list("global_intensity").alias("global_intensity"),
#         collect_list("sub_metering_1").alias("sub_metering_1"),
#         collect_list("sub_metering_2").alias("sub_metering_2"),
#         collect_list("sub_metering_3").alias("sub_metering_3"),
#     )
#
#     # Iterate over the grouped DataFrame and save each group to HDFS
#     for row in grouped_df.collect():
#         year = row['year']
#         month = row['month']
#         day = row['day']
#         hour = row['hour']
#         minute = row['minute']
#         timestamps = row['timestamps']
#         global_active_power = row['global_active_power']
#         global_reactive_power = row['global_reactive_power']
#         voltage = row['voltage']
#         global_intensity = row['global_intensity']
#         sub_metering_1 = row['sub_metering_1']
#         sub_metering_2 = row['sub_metering_2']
#         sub_metering_3 = row['sub_metering_3']
#
#         # Create a structured folder path based on the year, month, day, hour, and minute
#         output_dir_path = f'{HDFS_OUTPUT_DIR}/years={year}/months={month}/days={day}/hours={hour}/minutes={minute}'
#
#         # Create directories if they don't exist
#         os.makedirs(output_dir_path, exist_ok=True)
#
#         # Define the output file path
#         output_file_path = os.path.join(output_dir_path, 'file.json')
#
#         # Combine the lists of values into a DataFrame and write to HDFS
#         data = list(zip(timestamps, global_active_power, global_reactive_power, voltage, global_intensity,
#                         sub_metering_1, sub_metering_2, sub_metering_3))
#
#         columns = ['timestamp', 'global_active_power', 'global_reactive_power', 'voltage', 'global_intensity',
#                    'sub_metering_1', 'sub_metering_2', 'sub_metering_3']
#         group_df = spark.createDataFrame(data, columns)
#
#         # Write the grouped DataFrame to HDFS
#         group_df.coalesce(1).write.mode('overwrite').json(output_file_path)
#
#         # group_df.write.mode('overwrite').json(output_file_path)
#
#     print(f"Batch {batch_id} written successfully to HDFS with the structured folder paths.")


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

    print(raw_data)

    # Parse Kafka data
    json_data = raw_data.select(from_json(col("value").cast("string"), schema).alias("data"))

    print("Parsed data: ", json_data)

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

    print('********')
    print(parsed_data)
    print('********')

    # Write to HDFS
    query = parsed_data.writeStream \
        .foreachBatch(write_to_hdfs) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
