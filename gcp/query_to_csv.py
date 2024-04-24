from google.cloud import bigquery
import pandas as pd
import csv


PROJECT_ID = 'XXXXX'


# Instantiate a client
client = bigquery.Client(project=PROJECT_ID)

# Execute the query to retrieve all rows from the table
query = '''Select * from ods.ongage_contacts where email='igal.emona@gmail.com';'''

query_job = client.query(query)
results = query_job.result()

destination_file='/Users/iemona/temp/test.csv'

with open(destination_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([field.name for field in results.schema])
    for row in results:
        writer.writerow(row)