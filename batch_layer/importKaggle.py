import pandas as pd
import os

# After you run this: copy files to docker by running this in your local terminal
#  docker cp .\electrical_data\ docker_name:/tmp/electrical_data
# In the docker terminal, run this to upload files to hdfs
#  hadoop fs -mkdir electrical_data
#  hadoop fs -put /tmp/electrical_data/* /electrical_data

csv_file_path = 'household_power_consumption.csv'
df = pd.read_csv(csv_file_path)

df['timestamp'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str), dayfirst=True)
df['Global_active_power'] = pd.to_numeric(df['Global_active_power'], errors='coerce')
df['Global_reactive_power'] = pd.to_numeric(df['Global_reactive_power'], errors='coerce')
df['Voltage'] = pd.to_numeric(df['Voltage'], errors='coerce')
df['Global_intensity'] = pd.to_numeric(df['Global_intensity'], errors='coerce')
df['Sub_metering_1'] = pd.to_numeric(df['Sub_metering_1'], errors='coerce')
df['Sub_metering_2'] = pd.to_numeric(df['Sub_metering_2'], errors='coerce')
df['Sub_metering_3'] = pd.to_numeric(df['Sub_metering_3'], errors='coerce')

df.dropna(inplace=True)

for hour, group in df.groupby(df['timestamp'].dt.floor('T')):
    # Construct the output directory path (partitioned by year, month, day, hour, and minute)
    output_dir_path = f'electrical_data/year={hour.strftime("%Y")}/month={hour.strftime("%m")}/day={hour.strftime("%d")}/hour={hour.strftime("%H")}/minute={hour.strftime("%M")}'
    os.makedirs(output_dir_path, exist_ok=True)
    output_file_path = f'{output_dir_path}/file.parquet'
    group.to_parquet(output_file_path, engine='pyarrow', index=False, use_deprecated_int96_timestamps=True)

    print(f'Generated Parquet file for hour {hour}: {output_file_path}')
