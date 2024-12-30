CREATE KEYSPACE IF NOT EXISTS electrical
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE electrical;

CREATE TABLE IF NOT EXISTS sensor (
    sensor_id int,
    PRIMARY KEY (sensor_id)
);

CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id int,
    time timestamp,
    global_active_power double,
    global_reactive_power double,
    voltage double,
    global_intensity double,
    sub_metering_1 double,
    sub_metering_2 double,
    sub_metering_3 double,
    PRIMARY KEY ((sensor_id), time)
) WITH CLUSTERING ORDER BY (time DESC) AND default_time_to_live=300;

CREATE TABLE IF NOT EXISTS aggregated_sensor_data (
    sensor_id int,
    start_time timestamp,
    end_time timestamp,
    avg_global_active_power double,
    max_global_active_power double,
    min_global_active_power double,
    avg_global_reactive_power double,
    avg_voltage double,
    max_voltage double,
    avg_global_intensity double,
    total_sub_metering_1 double,
    total_sub_metering_2 double,
    total_sub_metering_3 double,
    PRIMARY KEY ((sensor_id), start_time)
) WITH CLUSTERING ORDER BY (start_time DESC);