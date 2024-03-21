from datetime import datetime
from google.cloud import bigquery
from google.cloud import datastream_v1alpha1 as datastream
import mysql.connector
import pandas as pd



# Set your project and location
project_id = "xxxxxx"
location = "xxxxxx"
parent = f"projects/{project_id}/locations/{location}"

# Instantiate a client
client = bigquery.Client()

# Define your project ID and dataset ID
# project_id = 'TangoMe-Production'
# dataset_id = 'ods_mysql'

client = bigquery.Client()

# Get the table
# table = client.get_table(table_ref)

# Execute the query to retrieve all rows from the table
query = '''Select table_name, IFNULL(upsert_stream_apply_watermark, CURRENT_TIMESTAMP()) as upsert_stream_apply_watermark
            from tangome-production.ods_mysql.INFORMATION_SCHEMA.TABLES t
            order by 2 desc;'''

query_job = client.query(query)
results = query_job.result()

# Iterate over the rows and print the values
bq_assets = {}
for row in results:
    bq_assets[row.table_name] = row.upsert_stream_apply_watermark.strftime('%Y-%m-%d %H:%M:%S')

## ======== MYSQL ==============
# Connect to the mysql database
db_connection = mysql.connector.connect(
    host="rdb-datalake.tangome.cloud",
    database="stream",
    user="bi",
    password="c5IgCdxbC8t13gAu"
)    

mysql_query = '''select CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) as TABLE_NAME, IFNULL(UPDATE_TIME, CURRENT_DATE) 
FROM information_schema.TABLES
'''

cursor = db_connection.cursor()
msql_query_job = cursor.execute(mysql_query)
data_mysql = cursor.fetchall()

mysql_assets = {}

for i in data_mysql:
    mysql_assets[i[0].replace('.', '_')] = {'updated': i[1].strftime('%Y-%m-%d %H:%M:%S'), 'name': i[0]}

# print(mysql_assets)

# Close the cursor and connection
cursor.close()
db_connection.close()

# ===== Datastream assets =========
# Initialize the Datastream client
client = datastream.DatastreamClient()

# List the streams in your project and location
streams = client.list_streams(parent=parent)

# Iterate over the streams and print the list of tables
stream_asset = []
for stream in streams:
    # print(f"Stream: {stream.name}")
    # print("Tables being streamed:")
    for database in stream.source_config.mysql_source_config.allowlist.mysql_databases:
        for table in database.mysql_tables:
            # print(f"{database.database_name}.{table.table_name}")
            stream_asset.append(f"{database.database_name}_{table.table_name}")



## ========== Check ETA and missing assets =====================
# print(mysql_assets.keys())

data = []

for asset in bq_assets:
    # print(asset)
    try:
        sql_table_name = mysql_assets[asset]['name']
        mysql_dt = datetime.strptime(mysql_assets[asset]['updated'], '%Y-%m-%d %H:%M:%S')
        bq_dt = datetime.strptime(bq_assets[asset], '%Y-%m-%d %H:%M:%S')
        time_diff = mysql_dt - bq_dt
        time_diff_min = time_diff.total_seconds() / 60
        
        if asset in stream_asset:
            isDatastream = True
        else:
            isDatastream = False

        if asset in mysql_assets and time_diff_min >= 3600:
            row = [sql_table_name, asset, mysql_dt, bq_dt, True, True, time_diff_min, isDatastream]
            data.append(row)
    except:
        continue
        # row = [None, asset, 'N/A', bq_assets[asset], False, True, None, isDatastream]
        # data.append(row)

df = pd.DataFrame(data, columns=['mySql_table', 'BQ_table', 'mySql_updated', 'BQ_updated', 'is_in_mySQL', 'is_in_BQ', 'time_diff_min', 'isDatastream'])
print(df)

csv_path = '/Users/iemona/temp/ods2.csv'
df.to_csv(csv_path,index=False)
