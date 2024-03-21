from google.cloud import bigquery
import mysql.connector


# Instantiate a client
client = bigquery.Client()

# Execute the query to retrieve all rows from the table
query = '''Select * from xxxx;'''

query_job = client.query(query)
results = query_job.result()

# Iterate over the rows and print the values
bq_assets = {}
for row in results:
    bq_assets[row.table_name] = row.upsert_stream_apply_watermark.strftime('%Y-%m-%d %H:%M:%S')

print(bq_assets)