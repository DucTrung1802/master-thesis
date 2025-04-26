import psycopg2

conn = psycopg2.connect(
    database="postgres",
    host="localhost",
    user="postgres",
    password="changeme",
    port="5432",
)

cursor = conn.cursor()

cursor.execute("SELECT version();")

print("PostgreSQL version:", cursor.fetchone())
