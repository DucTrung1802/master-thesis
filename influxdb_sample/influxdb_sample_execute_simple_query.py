import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

token = os.environ.get("INFLUXDB_TOKEN")
org = "DucTrung18WS"
url = "http://localhost:8086"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

query_api = write_client.query_api()

query = """from(bucket: "root")
  |> range(start: -1m)
  |> filter(fn: (r) => r["_measurement"] == "go_info")"""
tables = query_api.query(query)

for table in tables:
    for record in table.records:
        print(record)
