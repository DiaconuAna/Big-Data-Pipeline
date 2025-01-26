from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, avg, window, max, min, sum, from_unixtime, unix_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka:9092"
topic = "electrical_read"

schema = StructType([
    StructField("time", LongType(), True),
    StructField("global_active_power", DoubleType(), True),
    StructField("global_reactive_power", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("global_intensity", DoubleType(), True),
    StructField("sub_metering_1", DoubleType(), True),
    StructField("sub_metering_2", DoubleType(), True),
    StructField("sub_metering_3", DoubleType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed_df_with_id = parsed_df.withColumn("sensor_id", lit(1))
parsed_df_with_timestamp = parsed_df_with_id.withColumn(
    'timestamp',
    from_unixtime(col('time') / 1000).cast(TimestampType())  # Convert to seconds and cast to Timestamp
)

parsed_df_with_watermark = parsed_df_with_timestamp.withWatermark("timestamp", "1 minute")

# Real-time aggregation for speed layer
aggregated_df = parsed_df_with_watermark.groupBy(
    col("sensor_id"),
    window(col("timestamp"), "1 minute")
).agg(
    avg("global_active_power").alias("avg_global_active_power"),
    max("global_active_power").alias("max_global_active_power"),
    min("global_active_power").alias("min_global_active_power"),
    avg("global_reactive_power").alias("avg_global_reactive_power"),
    avg("voltage").alias("avg_voltage"),
    max("voltage").alias("max_voltage"),
    avg("global_intensity").alias("avg_global_intensity"),
    sum("sub_metering_1").alias("total_sub_metering_1"),
    sum("sub_metering_2").alias("total_sub_metering_2"),
    sum("sub_metering_3").alias("total_sub_metering_3")
)

final_aggregated_df = aggregated_df.select(
    col("sensor_id"),
    col("window.start").alias("start_time").cast("timestamp"),
    col("window.end").alias("end_time").cast("timestamp"),
    col("avg_global_active_power"),
    col("max_global_active_power"),
    col("min_global_active_power"),
    col("avg_global_reactive_power"),
    col("avg_voltage"),
    col("max_voltage"),
    col("avg_global_intensity"),
    col("total_sub_metering_1"),
    col("total_sub_metering_2"),
    col("total_sub_metering_3")
)

# Write aggregated results to Cassandra
final_query = final_aggregated_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/spark/checkpoint/aggregated_sensor_data") \
    .option("keyspace", "electrical") \
    .option("table", "aggregated_sensor_data") \
    .outputMode("append") \
    .start()

query = parsed_df_with_id.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/spark/checkpoint") \
    .option("keyspace", "electrical") \
    .option("table", "sensor_data") \
    .outputMode("append") \
    .start()

query.awaitTermination()
final_query.awaitTermination()
