import influxdb_client, os
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta

# Initialize client
token = os.environ.get("INFLUXDB_TOKEN")
org = "DucTrung18WS"
url = "http://localhost:8086"
bucket = "root"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Start and end time
start_time = datetime(2025, 1, 1, 0, 0, 0)
end_time = datetime(2025, 1, 1, 1, 0, 0)

time_interval = timedelta(minutes=5)

# Generate and write points
current_time = start_time
while current_time <= end_time:
    p = (
        influxdb_client.Point("my_measurement")
        .tag("name", "Trung")
        .field("age", 24)
        .time(current_time)
    )
    write_api.write(bucket=bucket, org=org, record=p)
    current_time += time_interval

print("Data points added successfully!")
