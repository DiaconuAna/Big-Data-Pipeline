from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, from_unixtime, collect_list, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "electrical_read"
HDFS_OUTPUT_DIR = "hdfs://namenode:8020/electrical_data/"

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
    # Convert the timestamp to a readable format
    batch_df = batch_df.withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp"))

    # Perform 1-minute aggregation with watermarking
    aggregated_df = batch_df.withWatermark("timestamp", "2 minutes").groupBy(
        window(col("timestamp"), "1 minute")
    ).agg(
        avg("global_active_power").alias("global_active_power"),
        avg("global_reactive_power").alias("global_reactive_power"),
        avg("voltage").alias("voltage"),
        avg("global_intensity").alias("global_intensity"),
        sum("sub_metering_1").alias("sub_metering_1"),
        sum("sub_metering_2").alias("sub_metering_2"),
        sum("sub_metering_3").alias("sub_metering_3")
    )

    # Add partitioning columns from the window
    aggregated_df = aggregated_df.withColumn("timestamp", col("window.start")) \
        .withColumn("year", date_format(col("window.start"), "yyyy")) \
        .withColumn("month", date_format(col("window.start"), "MM")) \
        .withColumn("day", date_format(col("window.start"), "dd")) \
        .withColumn("hour", date_format(col("window.start"), "HH")) \
        .withColumn("minute", date_format(col("window.start"), "mm")) \
        .drop("window")

    # Write the aggregated data to HDFS in a Hive-compatible partitioned format
    aggregated_df.write \
        .mode("append") \
        .partitionBy("year", "month", "day", "hour", "minute") \
        .parquet(HDFS_OUTPUT_DIR)

    print(f"Batch {batch_id} written successfully to HDFS in aggregated format.")


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

    # Write to HDFS
    query = parsed_data.writeStream \
        .foreachBatch(write_to_hdfs) \
        .trigger(processingTime="1 minute") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
