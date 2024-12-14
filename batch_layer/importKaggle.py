import pandas as pd
import os

# After you run this: copy files to docker by running this in your local terminal
#  docker cp .\electrical_data\ docker_name:/tmp/electrical_data
# In the docker terminal, run this to upload files to hdfs
#  hadoop fs -mkdir electrical_data
#  hadoop fs -put /tmp/electrical_data/* electrical_data

csv_file_path='household_power_consumption.csv'
df = pd.read_csv(csv_file_path)

df['Global_active_power'] = pd.to_numeric(df['Global_active_power'], errors='coerce')
df['Global_reactive_power'] = pd.to_numeric(df['Global_reactive_power'], errors='coerce')
df['Voltage'] = pd.to_numeric(df['Voltage'], errors='coerce')
df['Global_intensity'] = pd.to_numeric(df['Global_intensity'], errors='coerce')
df['Sub_metering_1'] = pd.to_numeric(df['Sub_metering_1'], errors='coerce')
df['Sub_metering_2'] = pd.to_numeric(df['Sub_metering_2'], errors='coerce')
df['Sub_metering_3'] = pd.to_numeric(df['Sub_metering_3'], errors='coerce')

df.dropna(inplace=True)

df['timestamp'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str), dayfirst=True)

for hour, group in df.groupby(df['timestamp'].dt.floor('T')):
    output_dir_path = f'electrical_data/years={hour.strftime("%Y")}/months={hour.strftime("%m")}/days={hour.strftime("%d")}/hours={hour.strftime("%H")}/minutes={hour.strftime("%M")}'
    os.makedirs(output_dir_path, exist_ok=True)
    output_file_path = f'{output_dir_path}/file.json'
    group.drop(columns=['timestamp'], inplace=True)
    group.to_json(output_file_path, orient='records', lines=True)

    print(f'Generated JSON file for hour {hour}: {output_file_path}')
