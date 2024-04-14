from google.cloud import datastream_v1alpha1 as datastream

# Initialize the Datastream client
client = datastream.DatastreamClient()

# Set your project and location
project_id = "XXXXX"
location = "XXXXX"
parent = f"projects/{project_id}/locations/{location}"

# List the streams in your project and location
streams = client.list_streams(parent=parent)

# Iterate over the streams and print the list of tables
stream_asset = []
TABLE_LIST = ['family_family_member', 'family_family','stickaloger_gift_collection_mapping']

for stream in streams:
    for database in stream.source_config.mysql_source_config.allowlist.mysql_databases:
        for table in database.mysql_tables:
            stream_name = stream.name.rsplit('/',1)[-1]
            table = table.table_name
            db = database.database_name 
            print(db)
            bq_table = db + '_' + table
            # print(stream_name + ': ' + db + '_' + table)
            if bq_table in TABLE_LIST:
                print(stream_name + ': ' + db + '_' + table)

            