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
) WITH CLUSTERING ORDER BY (time DESC);
