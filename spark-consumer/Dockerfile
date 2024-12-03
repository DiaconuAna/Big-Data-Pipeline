FROM bde2020/spark-master:3.0.0-hadoop3.2-java11

# Copy the spark-consumer folder into the container
COPY . /app
WORKDIR /app

# Install required Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Spark submit command to run the consumer
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0", "spark_consumer.py"]
