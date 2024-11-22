import math
import random
import time
from cassandra.cluster import Cluster

min_global_active_power = 0.08
max_global_active_power = 10.7
min_global_reactive_power = 0
max_global_reactive_power = 1.15
min_voltage = 223
max_voltage = 251
min_global_intensity = 0.4
max_global_intensity = 46.4
min_sub_metering_1 = 0
max_sub_metering_1 = 78
min_sub_metering_2 = 0
max_sub_metering_2 = 78
min_sub_metering_3 = 0
max_sub_metering_3 = 20

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect("electrical")


def generate_set(old_global_active_power, old_global_reactive_power, old_voltage, old_global_intensity,
                 old_sub_metering_1, old_sub_metering_2, old_sub_metering_3):
    global_active_power = generate_new_value(old_global_active_power, 1, min_global_active_power,
                                             max_global_active_power)
    global_reactive_power = generate_new_value(old_global_reactive_power, 0.1, min_global_reactive_power,
                                               max_global_reactive_power)
    voltage = generate_new_value(old_voltage, 0.5, min_voltage, max_voltage)
    global_intensity = generate_new_value(old_global_intensity, 1, min_global_intensity, max_global_intensity)
    sub_metering_1 = generate_new_value(old_sub_metering_1, 1, min_sub_metering_1, max_sub_metering_1)
    sub_metering_2 = generate_new_value(old_sub_metering_2, 1, min_sub_metering_2, max_sub_metering_2)
    sub_metering_3 = generate_new_value(old_sub_metering_3, 0.2, min_sub_metering_3, max_sub_metering_3)

    return global_active_power, global_reactive_power, voltage, global_intensity, sub_metering_1, sub_metering_2, sub_metering_3


def generate_new_value(old_value, margin, min, max):
    upper_value = math.floor((old_value + margin) * 1000)
    lower_value = math.floor((old_value - margin) * 1000)

    result = random.randint(lower_value, upper_value) / 1000
    if result < min:
        result = min
    elif result > max:
        result = max

    return result


if __name__ == '__main__':
    global_active_power = 5
    global_reactive_power = 0.5
    voltage = 240
    global_intensity = 23
    sub_metering_1 = 39
    sub_metering_2 = 39
    sub_metering_3 = 10

    while True:
        global_active_power, global_reactive_power, voltage, global_intensity, sub_metering_1, sub_metering_2, sub_metering_3 = generate_set(
            global_active_power, global_reactive_power, voltage, global_intensity, sub_metering_1, sub_metering_2,
            sub_metering_3)

        t = int(time.time() * 1000)

        session.execute(
            "INSERT INTO sensor_data (sensor_id, time, global_active_power, global_intensity, global_reactive_power, sub_metering_1, sub_metering_2, sub_metering_3, voltage) VALUES (1, " + str(
                t) + "," + str(global_active_power) + "," + str(global_intensity) + "," + str(
                global_reactive_power) + "," + str(sub_metering_1) + "," + str(sub_metering_2) + "," + str(
                sub_metering_3) + "," + str(voltage) + ");")
        print(t, global_active_power, global_reactive_power, voltage, global_intensity, sub_metering_1,
              sub_metering_2, sub_metering_3)
        time.sleep(1)
