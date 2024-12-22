import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.environ.get("INFLUXDB_TOKEN")
org = "DucTrung18WS"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

bucket = "root"

write_api = write_client.write_api(write_options=SYNCHRONOUS)

for value in range(5):
    point = Point("measurement1").field("field1", value).tag("tagname1", "tagvalue1")
    write_api.write(bucket=bucket, org="DucTrung18WS", record=point)
    time.sleep(1)  # separate points by 1 second
