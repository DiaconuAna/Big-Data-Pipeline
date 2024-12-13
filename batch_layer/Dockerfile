FROM bitnami/spark:latest

WORKDIR /opt/spark

COPY batch_layer.py /opt/spark/batch_layer.py
COPY requirements.txt /opt/spark/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Set entrypoint to run the Spark job with the proper master

CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:2.8.0",  "/opt/spark/batch_layer.py"]
