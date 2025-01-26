# Electrical Consumption Monitoring - Big Data Pipeline using Lambda Architecture

This project implements a Big Data pipeline based on Lambda Architecture to monitor and analyze household
electrical consumption. It uses both historical and real-time data to provide insights into electricity usage.

The purpose of this pipeline is to help users track their electricity consumption and explore ways to optimize it
by visualizing usage trends over time. This includes data from simulated smart plugs tracking various parts of a household
and overall consumption readings.

## Architecture Overview

The project uses Lambda Architecture, a data deployment model for processing consisting of a 
traditional batch data pipeline and a fast streaming data pipeline for handling real-time data. In addition to
the batch and speed layers, Lambda Architecture includes a data serving layer for responding to user queries.

The overall architecture is illustrated in the diagram below:

![lambda_overview](https://github.com/DiaconuAna/Big-Data-Pipeline/blob/main/Resources/Lambda_architecture.png)

The project is divided into the following layers:

1. **Ingestion Layer**
   - **Historical Data**: Sourced from the [Kaggle Electrical Consumption Dataset](https://www.kaggle.com/datasets/imtkaggleteam/household-power-consumption).
   - **Real-time Data**: Simulated using Apache Kafka to emulate sensor data in the same schema as the historical dataset.


2. **Batch Layer**
   - Processes historical data stored in HDFS using Apache Spark to compute aggregated statistics. The sensor generates data per second while, in the batch layer, data is aggregated over the time period of a minute.
   - Outputs the data in Parquet format in preparation for the serving layer.

3. **Speed Layer**
   - Processes real-time sensor data using PySpark and stores it in Cassandra.
   - Aggregates statistics (averages, max, min, sum) per minute and stores the data in Cassandra.
  
4. **Serving Layer**
   - Combines outputs from both layers.
   - Uses:
     - Apache Hive for querying batch views.
     - Grafana for batch and real-time visualizations with connectors to Cassandra (for the speed layer) and Trino (for the batch layer).
     - Python scripts querying Hive as a fallback for the batch views.

Here are a couple of visualizations for both batch and real-time views.

<p align="center"><b>Batch Views in Python using pandas and matplotlib</b></p>
<p align="center">
  <img src="https://github.com/DiaconuAna/Big-Data-Pipeline/blob/main/Resources/pftime.png" alt="Power Factor Over Time" width="45%">
  <img src="https://github.com/DiaconuAna/Big-Data-Pipeline/blob/main/Resources/voltage.png" alt="Voltage Over Time" width="45%">
</p>

<p align="center"><b>Batch Views in Grafana</b></p>

![batch_views](https://github.com/DiaconuAna/Big-Data-Pipeline/blob/main/Resources/batch_layer.png)

<p align="center"><b>Real-Time Views in Grafana</b></p>

![real_time_views](https://github.com/DiaconuAna/Big-Data-Pipeline/blob/main/Resources/speed_layer.png)

-------

## Technology stack

- **Data Ingestion**: 
  - Kaggle Dataset for historical data.
  - Apache Kafka for real-time data simulation.

- **Batch Layer**:
  - Apache Spark is used to process and aggregate historical data.
  - HDFS for storage.

- **Speed Layer**:
  - PySpark is used to process and aggregate real-time data.
  - Cassandra for storing real-time views.

- **Serving Layer**:
  - Apache Hive and Trino for querying aggregated data.
  - Grafana and Python for visualization.
   
